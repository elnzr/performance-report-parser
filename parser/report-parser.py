import os
import pandas as pd
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import matplotlib.pyplot as plt


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


def show_visa_types_plot(df, ax):
    # df.filter(F.col("CLASS_OF_ADMISSION").startswith('EB'))

    # df.filter(F.col("CLASS_OF_ADMISSION") != 'H-1B')
    #    .filter(F.col("CLASS_OF_ADMISSION") != "F-1")
    #    .filter(F.col("CLASS_OF_ADMISSION") != "Not in USA")
    #    .filter(F.col("CLASS_OF_ADMISSION") != "L-1")

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

    ax.pie(sizes, labels=labels)
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


def show_software_companies_with_max_cases_cnt_and_wage(df: DataFrame):
    # (
    #     df.filter(F.col('COUNTRY_OF_CITIZENSHIP').startswith("RUSSIA"))
    #         .filter(df.JOB_TITLE.like('Data Engineer'))
    #         .limit(10)
    #         .show(truncate=False)
    #  )

    # (
    #     df.groupBy(df.CASE_STATUS)
    #         .agg(F.countDistinct(df.CASE_NUMBER))
    #         .show(truncate=False)
    # )

    res_df = (
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
           .filter(df.WORKSITE_STATE == 'CALIFORNIA')
           .filter(
               F.lower(df.WORKSITE_CITY).isin(
                   'los angeles', 'irvine', 'orange county', 'pasadena', 'el segundo', 'santa monica',
                   'culver city', 'glendale', 'woodland hills', 'newport beach'
               )
           )
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


def init_spark_session():
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .getOrCreate()

    return spark


def main():
    xlsx_report_path = os.environ['xlsx_report_path']
    csv_report_name = 'perf-report.csv'
    if not os.path.isfile(csv_report_name):
        print('Csv report not found, run xlsx -> csv transformation')
        transform_xlsx_to_csv(xlsx_report_path, csv_report_name)
    else:
        print('Csv report found, skip xlsx -> csv transformation')

    spark = init_spark_session()
    df = parse_report(spark, csv_report_name)

    fig, axis = plt.subplots(2, 2)
    # show_software_companies_with_max_cases_cnt_and_wage(df)
    show_visa_types_plot(df, axis[0, 0])
    show_employers_cnt_per_city(df, axis[0, 1])

    fig.subplots_adjust(hspace=0.1, wspace=0.5)
    plt.show()


if __name__ == "__main__":
    main()
