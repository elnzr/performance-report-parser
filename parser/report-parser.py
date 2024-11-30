import os
from functools import reduce

import pandas as pd
from pyspark.sql import SparkSession, DataFrame, Column
import pyspark.sql.functions as F
import matplotlib.pyplot as plt
import seaborn as sns
from typing import List


# SET PYSPARK_PYTHON env to <your-path>\performance-report-parser\venv\Scripts\python.exe

def transform_xlsx_to_csv(xlsx_report_path, csv_report_name="report-csv.csv"):
    if not os.path.isfile(xlsx_report_path):
        raise ValueError(f'xlsx report was not found under "{xlsx_report_path}"! Exit with error')
    else:
        print('xlsx report found')

    # read by default 1st sheet of an Excel file
    df = pd.read_excel(xlsx_report_path, engine='openpyxl')
    df.to_csv(csv_report_name)


def parse_report(spark, csv_report_path) -> DataFrame:
    df = spark.read.csv(csv_report_path, header=True)
    df.printSchema()
    df.limit(3).show(truncate=False)

    return df


def show_visa_types(df, ax):
    res_df = (
        df.groupBy(F.col("CLASS_OF_ADMISSION"))
          .agg(F.countDistinct(F.col("CASE_NUMBER")))
          .orderBy(F.col("count(CASE_NUMBER)").desc())
          .limit(15)
    )
    res_df.show(truncate=False)

    res_pd = res_df.toPandas()
    labels = res_pd['CLASS_OF_ADMISSION']
    sizes = res_pd['count(CASE_NUMBER)']

    ax.pie(sizes, labels=labels, colors=sns.color_palette("light:lawngreen"))
    ax.set_title('Rows count per visa type')
    ax.legend(labels[:15], bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0)


def show_employers_cnt_per_city(df: DataFrame, ax):
    transformed_df = (
        df.filter(df.CASE_STATUS == 'Certified')
            .filter(
                (F.lower(df.JOB_TITLE).like('%software engineer%')
                 | F.lower(df.JOB_TITLE).like('%data engineer%')
                 | F.lower(df.JOB_TITLE).like('%software developer%'))
                |
                (F.lower(df.SPECIFIC_SKILLS).like('%spark%'))
                |
                (F.lower(df.PW_SOC_TITLE).like('%data%')
                 | F.lower(df.PW_SOC_TITLE).like('%software developer%')
                 | F.lower(df.PW_SOC_TITLE).like('%software engineer%'))
                |
                (F.lower(df.ACCEPT_ALT_JOB_TITLE).like('%data%')
                 | F.lower(df.ACCEPT_ALT_JOB_TITLE).like('%software developer%')
                 | F.lower(df.ACCEPT_ALT_JOB_TITLE).like('%software engineer%'))
            )
    )
    companies_per_city_df = (
        transformed_df.groupBy(F.lower(df.WORKSITE_CITY).alias('city'))
            .agg(F.countDistinct(df.EMPLOYER_NAME).alias("employers_cnt"))
            .orderBy(F.col('employers_cnt').desc())
    )
    print(f"Found {companies_per_city_df.count()} cities")
    companies_per_city_df.limit(50).show(55, truncate=False)

    res_pd = companies_per_city_df.limit(15).toPandas()
    labels = res_pd['city']
    values = res_pd['employers_cnt']

    ax.barh(labels, values, color='lawngreen')

    # Show top values
    ax.invert_yaxis()

    # Remove x, y Ticks
    ax.xaxis.set_ticks_position('none')
    ax.yaxis.set_ticks_position('none')

    # Add padding between axes and labels
    ax.xaxis.set_tick_params(pad=5)
    ax.yaxis.set_tick_params(pad=10)

    # Add x, y gridlines
    ax.grid(b=True, color='grey',
            linestyle='-.', linewidth=0.5,
            alpha=0.2)

    # Add annotation to bars
    for i in ax.patches:
        ax.text(i.get_width() + 0.2, i.get_y() + 0.5,
                 str(round((i.get_width()), 2)),
                 fontsize=10, fontweight='bold', color='grey')

    ax.set_xlabel('No. of employers')
    ax.set_ylabel('Cities')
    ax.title.set_text("Top 15 cities with the greatest employers count")


def show_software_companies_with_max_cases_cnt_and_wage(df: DataFrame, job_titles, skills, state, cities, ax_cases,
                                                        ax_wage):
    def contains_any(c: Column, values: List[str]):
        lower_values = [c.like(f'%{j.lower()}%') for j in values]
        or_like_condition = reduce(lambda x, y: x | y, lower_values)
        return or_like_condition

    lower_cities = [c.lower() for c in cities]
    tdf = (
        df.filter(df.CASE_STATUS == 'Certified')
            .filter(df.CLASS_OF_ADMISSION == 'H-1B')
            .withColumn('lower_job_title', F.lower(df.JOB_TITLE))
            .withColumn('lower_pw_soc_title', F.lower(df.PW_SOC_TITLE))
            .withColumn('lower_alt_job_title', F.lower(df.ACCEPT_ALT_JOB_TITLE))
    )
    res_df = (
        tdf.filter(
            contains_any(tdf.lower_job_title, job_titles)
                | contains_any(tdf.lower_pw_soc_title, job_titles)
                | contains_any(tdf.lower_alt_job_title, job_titles)
                | contains_any(tdf.SPECIFIC_SKILLS, skills)
            )
            .filter(F.upper(tdf.WORKSITE_STATE) == state.upper())
            .filter(F.lower(tdf.WORKSITE_CITY).isin(*lower_cities))
    )
    print(f"Found {res_df.count()} cases")
    res_df.show(truncate=False)

    companies_ordered_df = (
        res_df.groupBy('EMPLOYER_NAME')
            .agg(F.count(F.col('CASE_NUMBER')).alias('cases_cnt'), F.max(F.col('PW_WAGE')).alias('max_wage'))
            .orderBy(F.col('cases_cnt').desc(), F.col('max_wage').desc(), F.col('EMPLOYER_NAME'))
    )
    print(f"Found {companies_ordered_df.count()} employers")
    companies_ordered_df.limit(50).show(50, truncate=False)

    companies_df = companies_ordered_df.limit(20)

    pd_df = companies_df.toPandas()
    pd_df = pd_df.iloc[::-1]
    pd_df['max_wage'] = pd_df['max_wage'].astype(float)

    pd_df[["EMPLOYER_NAME", "cases_cnt"]].plot(x="EMPLOYER_NAME", kind="barh", ax=ax_cases, fontsize=6, color='lawngreen')
    pd_df[["EMPLOYER_NAME", "max_wage"]].plot(x="EMPLOYER_NAME", kind="barh", ax=ax_wage, fontsize=6, color='lawngreen')

    ax_cases.set_xlabel('Cases count')
    ax_cases.set_ylabel('Company')
    ax_cases.set_title("Top 15 companies with the greatest cases count")
    ax_cases.get_legend().set_visible(False)

    ax_wage.set_xlabel('Wage')
    ax_wage.set_ylabel('Company')
    ax_wage.set_title("Wage in the companies with highest cases count")
    ax_wage.get_legend().set_visible(False)


def init_spark_session():
    spark = SparkSession \
        .builder \
        .appName("Basic example") \
        .getOrCreate()

    return spark


def main():
    xlsx_report_path = os.environ.get('xlsx_report_path', None)
    if not xlsx_report_path:
        raise ValueError(f"Path to the input xlsx report not set! Set it in env var: 'xlsx_report_path'")

    csv_report_name = 'performance-report.csv'
    if not os.path.isfile(csv_report_name):
        print('Csv report not found, run xlsx -> csv transformation')
        transform_xlsx_to_csv(xlsx_report_path, csv_report_name)
    else:
        print('Csv report found, skip xlsx -> csv transformation')

    spark = init_spark_session()
    df = parse_report(spark, csv_report_name)

    fig, axis = plt.subplots(2, 2)

    job_titles = ['Software Engineer', 'Data Engineer', 'Software Developer', 'Data']
    state = 'California'
    cities = ['los angeles', 'irvine', 'orange county', 'pasadena', 'el segundo', 'santa monica',
        'culver city', 'glendale', 'woodland hills', 'newport beach']
    skills = ['Spark']
    show_software_companies_with_max_cases_cnt_and_wage(df, job_titles, skills, state, cities, axis[1, 0], axis[1, 1])
    show_visa_types(df, axis[0, 0])
    show_employers_cnt_per_city(df, axis[0, 1])

    plt.subplots_adjust(left=0.17, right=0.97, top=0.9, bottom=0.06, hspace=0.56, wspace=0.53)
    plt.show()


if __name__ == "__main__":
    main()
