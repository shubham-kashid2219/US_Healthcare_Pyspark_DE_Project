from pyspark.sql.window import Window
from pyspark.sql.functions import upper, size, sum, count_distinct, dense_rank, col
from udfs import column_split_cnt
import logging
import logging.config

### Load the Logging Configuration File
logging.config.fileConfig(fname='../util/logging_to_file.conf')
logger = logging.getLogger(__name__)


def city_report(df_city_sel, df_fact_sel):
    '''
    # City Report:
        Transform logic:
            1. Calculate the number of zips in each city
            2. Calculate the number of distinct Prescribers assigned to each city.
            3. Calculate total TRX_CNT prescribed for each city.
            4. Do not report a city in the final report if no prescriber is assigned to it.

        Layout:
            City Name
            State Name
            County Name
            City Population
            Number Of zips
            Prescriber Counts
            Total TRX counts
    '''
    try:
        logger.info(f"Transform - city_report() is started ...")
        #df_city_cleaned = df_city_cleaned.dropna(subset="zips")
        df_city_split = df_city_sel.withColumn('zip_count', column_split_cnt(df_city_sel.zips))

        df_fact_grp = df_fact_sel \
            .groupby("presc_state", "presc_city") \
            .agg(count_distinct('presc_id').alias('presc_counts'), sum('trx_cnt').alias("trx_counts")) \
            .orderBy('presc_counts')

        df_city_join = df_city_split.join(df_fact_grp, (df_city_split.state_id == df_fact_grp.presc_state) \
                                          & (df_city_split.city == df_fact_grp.presc_city), 'inner')

        df_city_final = df_city_join.select("city", "state_name", \
                                            "county_name", "population", \
                                            "zip_count", "trx_counts", \
                                            "presc_counts")
    except Exception as exp:
        logger.error("Error in the method city_report(). Please check Stack Trace. " + str(exp), exc_info=True)
    else:
        logger.info("Trensform - city_report() is completed...")
    return df_city_final


def top_5_prescribers(df_fact_sel):

    """
    # Prescriber Report:
    Top 5 Prescribers with highest trx_cnt per each state.
    Consider the prescribers only from 20 to 50 years of experience.
    Layout:
        Prescriber ID
        Prescriber Full Name
        Prescriber State
        Prescriber Country
        Prescriber Years of Experience
        Total TRX Counts
        Total Days Supply
        Total Drug Cost
    """
    try:
        logger.info("Transform prescriber_report() is started")
        ### Filtering the Prescribers only from 20-30 years of experience.
        ### Ranking the Prescribers based on their trx_cnt for each state.
        ### Selecting top two prescribers from each state.
        spec1 = Window.partitionBy('presc_state').orderBy(col('trx_cnt').desc())
        df_presc_final = df_fact_sel.select('presc_id',
                                            'presc_fullname',
                                            'presc_state',
                                            'country_name',
                                            'years_of_exp',
                                            'trx_cnt',
                                            'total_day_supply',
                                            'total_drug_cost'
                                            ) \
            .filter((df_fact_sel.years_of_exp >= 20) & (df_fact_sel.years_of_exp <= 30)) \
            .orderBy(df_fact_sel['years_of_exp'].desc()) \
            .withColumn('dense_rank', dense_rank().over(spec1)) \
            .filter(col('dense_rank') <= 2)

        ### Ranking the Prescribers based on their trx_cnt for each state
        # spec1 = Window.partitionBy('presc_state').orderBy(col('trx_cnt').desc())
        # df_fact_rank = df_fact_filter.withColumn('presc_rank', dense_rank().over(spec1))

        ### Selecting top two prescribers from each state.
        # df_fact_final = df_fact_rank.filter(df_fact_rank.presc_rank <= 2)

    except Exception as exp:
        logger.error("Error in the method prescriber_report(). Please check stack trace" + str(exp), exc_info=True)
        raise
    else:
        logger.info("Transform - prescriber_report() is completed ")
    return df_presc_final


