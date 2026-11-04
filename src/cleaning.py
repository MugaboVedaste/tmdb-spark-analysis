
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def drop_irrelevant_columns(df: DataFrame) -> DataFrame:
    """
    Remove columns that are not required for movie analysis.

    Args:
        df (DataFrame): Raw Spark movie DataFrame.

    Returns:
        DataFrame: DataFrame without irrelevant columns.
    """

    columns_to_drop = [
        "adult",
        "imdb_id",
        "original_title",
        "video",
        "homepage",
        "softcore"
    ]

    cleaned_df = df.drop(*columns_to_drop)

    return cleaned_df


def extract_collection_name(df: DataFrame) -> DataFrame:
    """
    Extract the collection name from belongs_to_collection.
    """

    return df.withColumn(
        "belongs_to_collection",
        F.col("belongs_to_collection.name")
    )



def extract_genres(df: DataFrame) -> DataFrame:
    """
    Extract genre names and combine them using '|'.
    """

    return df.withColumn(
        "genres",
        F.concat_ws(
            "|",
            F.transform(
                F.col("genres"),
                lambda genre: genre["name"]
            )
        )
    )


def extract_spoken_languages(df: DataFrame) -> DataFrame:
    """
    Extract spoken language names and combine using '|'.
    """

    return df.withColumn(
        "spoken_languages",
        F.concat_ws(
            "|",
            F.transform(
                F.col("spoken_languages"),
                lambda language: language["name"]
            )
        )
    )


def extract_production_countries(df: DataFrame) -> DataFrame:
    """
    Extract production country names and combine using '|'.
    """

    return df.withColumn(
        "production_countries",
        F.concat_ws(
            "|",
            F.transform(
                F.col("production_countries"),
                lambda country: country["name"]
            )
        )
    )


def extract_production_companies(df: DataFrame) -> DataFrame:
    """
    Extract production company names and combine using '|'.
    """

    return df.withColumn(
        "production_companies",
        F.concat_ws(
            "|",
            F.transform(
                F.col("production_companies"),
                lambda company: company["name"]
            )
        )
    )



def extract_cast_and_size(df: DataFrame) -> DataFrame:
    """
    Extract cast names and calculate cast size.
    """

    cast_array = F.coalesce(
        F.col("credits.cast"),
        F.array()
    )

    df = df.withColumn(
        "cast",
        F.concat_ws(
            "|",
            F.transform(
                cast_array,
                lambda actor: actor["name"]
            )
        )
    )

    df = df.withColumn(
        "cast_size",
        F.size(cast_array)
    )

    return df



def extract_director_and_crew_size(df: DataFrame) -> DataFrame:
    """
    Extract director name and calculate crew size.
    """

    crew_array = F.coalesce(
        F.col("credits.crew"),
        F.array()
    )

    directors = F.filter(
        crew_array,
        lambda member: member["job"] == "Director"
    )

    df = df.withColumn(
        "director",
        F.element_at(
            F.transform(
                directors,
                lambda member: member["name"]
            ),
            1
        )
    )

    df = df.withColumn(
        "crew_size",
        F.size(crew_array)
    )

    return df


def drop_credits_column(df: DataFrame) -> DataFrame:
    """
    Remove the original nested credits column.
    """

    return df.drop("credits")

def convert_data_types(df):
    """
    Convert movie columns to appropriate data types.
    """

    df = df.withColumn(
        "release_date",
        F.to_date(F.col("release_date"), "yyyy-MM-dd")
    )

    df = df.withColumn("budget", F.col("budget").cast("long"))
    df = df.withColumn("revenue", F.col("revenue").cast("long"))
    df = df.withColumn("runtime", F.col("runtime").cast("integer"))
    df = df.withColumn("vote_average", F.col("vote_average").cast("double"))
    df = df.withColumn("vote_count", F.col("vote_count").cast("integer"))
    df = df.withColumn("popularity", F.col("popularity").cast("double"))

    return df

def missing_value_report(df: DataFrame) -> DataFrame:
    """
    Count missing values in each column.
    Missing means NULL or an empty string for string columns.
    """

    expressions = []

    for field in df.schema.fields:
        column_name = field.name

        if field.dataType.simpleString() == "string":
            missing_condition = (
                F.col(column_name).isNull()
                | (F.trim(F.col(column_name)) == "")
            )
        else:
            missing_condition = F.col(column_name).isNull()

        expressions.append(
            F.sum(
                F.when(missing_condition, 1).otherwise(0)
            ).alias(column_name)
        )

    return df.select(*expressions)
def handle_missing_values(df: DataFrame) -> DataFrame:
    """
    Replace missing collection names with 'No Collection'.
    """

    df = df.withColumn(
        "belongs_to_collection",
        F.when(
            F.col("belongs_to_collection").isNull()
            | (F.trim(F.col("belongs_to_collection")) == ""),
            "No Collection"
        ).otherwise(F.col("belongs_to_collection"))
    )

    return df

def check_unrealistic_values(df: DataFrame):
    """
    Count records containing unrealistic numeric values.
    """

    conditions = {
        "negative_budget": F.col("budget") < 0,
        "negative_revenue": F.col("revenue") < 0,
        "invalid_runtime": F.col("runtime") <= 0,
        "invalid_vote_average": (
            (F.col("vote_average") < 0)
            | (F.col("vote_average") > 10)
        ),
        "negative_vote_count": F.col("vote_count") < 0,
        "negative_popularity": F.col("popularity") < 0
    }

    return df.select([
        F.sum(
            F.when(condition, 1).otherwise(0)
        ).alias(name)
        for name, condition in conditions.items()
    ])

def duplicate_movie_report(df: DataFrame) -> DataFrame:
    """
    Identify duplicate movie IDs.
    """

    duplicates = (
        df.groupBy("id")
        .count()
        .filter(F.col("count") > 1)
    )

    return duplicates.agg(
        F.count("*").alias("duplicate_id_groups"),
        F.coalesce(
            F.sum(F.col("count") - 1), F.lit(0)
        ).alias("extra_duplicate_records")
    )

def remove_duplicate_movies(df: DataFrame) -> DataFrame:
    """
    Remove duplicate movies based on TMDB movie ID.
    """

    return df.dropDuplicates(["id"])

def final_data_quality_report(df: DataFrame) -> DataFrame:
    """
    Summarize the final cleaned dataset.
    """

    return df.agg(
        F.count("*").alias("total_movies"),
        F.countDistinct("id").alias("unique_movie_ids")
    )
def reorder_and_finalize_dataframe(df: DataFrame) -> DataFrame:
    """
    Convert budget and revenue to millions of USD,
    then select and reorder the required columns.
    """

    df = df.withColumn(
        "budget_musd",
        F.col("budget") / 1_000_000
    )

    df = df.withColumn(
        "revenue_musd",
        F.col("revenue") / 1_000_000
    )

    df = df.drop("budget", "revenue")

    final_columns = [
        "id",
        "title",
        "tagline",
        "release_date",
        "genres",
        "belongs_to_collection",
        "original_language",
        "budget_musd",
        "revenue_musd",
        "production_companies",
        "production_countries",
        "vote_count",
        "vote_average",
        "popularity",
        "runtime",
        "overview",
        "spoken_languages",
        "poster_path",
        "cast",
        "cast_size",
        "director",
        "crew_size"
    ]

    return df.select(*final_columns)