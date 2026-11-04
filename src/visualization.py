import matplotlib.pyplot as plt
from pathlib import Path
import seaborn as sns
from pyspark.sql import functions as F
# Create reports folder if it doesn't exist
REPORTS_DIR = Path(__file__).resolve().parent.parent / "reports"
REPORTS_DIR.mkdir(parents=True, exist_ok=True)

def plot_revenue_vs_budget(df):
    data = df.select(
        "title",
        "budget_musd",
        "revenue_musd"
    ).dropna().toPandas()

    plt.figure(figsize=(10, 6))

    sns.scatterplot(
        data=data,
        x="budget_musd",
        y="revenue_musd"
    )

    plt.title("Movie Revenue vs. Production Budget")
    plt.xlabel("Production Budget (Million USD)")
    plt.ylabel("Box Office Revenue (Million USD)")
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(REPORTS_DIR / "revenue_vs_budget.png", dpi=300, bbox_inches="tight")
    plt.show()


def plot_roi_by_genre(df):
    data = (
        df
        .filter(
            (F.col("budget_musd") > 0) &
            F.col("revenue_musd").isNotNull() &
            F.col("genres").isNotNull()
        )
        .withColumn(
            "roi",
            F.col("revenue_musd") / F.col("budget_musd")
        )
        .withColumn(
            "genre",
            F.explode(F.split(F.col("genres"), r"\|"))
        )
        .select("genre", "roi")
        .toPandas()
    )

    plt.figure(figsize=(12, 6))

    sns.boxplot(
        data=data,
        x="genre",
        y="roi"
    )

    plt.title("ROI Distribution by Genre")
    plt.xlabel("Movie Genre")
    plt.ylabel("ROI (Revenue / Budget)")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(REPORTS_DIR / "roi_by_genre.png", dpi=300, bbox_inches="tight")
    plt.show()


def plot_popularity_vs_rating(df):
    data = df.select(
        "title",
        "popularity",
        "vote_average"
    ).dropna().toPandas()

    plt.figure(figsize=(10, 6))

    sns.scatterplot(
        data=data,
        x="popularity",
        y="vote_average"
    )

    plt.title("Movie Popularity vs. Audience Rating")
    plt.xlabel("Popularity")
    plt.ylabel("Average Rating")
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(REPORTS_DIR / "popularity_vs_rating.png", dpi=300, bbox_inches="tight")
    plt.show()

def plot_yearly_box_office_trends(df):
    yearly_data = (
        df
        .filter(
            F.col("release_date").isNotNull()
        )
        .withColumn(
            "release_year",
            F.year("release_date")
        )
        .groupBy("release_year")
        .agg(
            F.sum("budget_musd").alias("total_budget"),
            F.sum("revenue_musd").alias("total_revenue")
        )
        .orderBy("release_year")
        .toPandas()
    )

    plt.figure(figsize=(12, 6))

    plt.plot(
        yearly_data["release_year"],
        yearly_data["total_budget"],
        marker="o",
        label="Total Budget"
    )

    plt.plot(
        yearly_data["release_year"],
        yearly_data["total_revenue"],
        marker="o",
        label="Total Revenue"
    )

    plt.title("Yearly Trends in Movie Budget and Box Office Revenue")
    plt.xlabel("Release Year")
    plt.ylabel("Amount (Million USD)")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(REPORTS_DIR / "yearly_box_office_trends.png", dpi=300, bbox_inches="tight")
    plt.show()

def plot_franchise_vs_standalone(df):
    data = (
        df
        .withColumn(
            "movie_type",
            F.when(
                F.col("belongs_to_collection") == "No Collection",
                "Standalone"
            ).otherwise("Franchise")
        )
        .withColumn(
            "roi",
            F.when(
                F.col("budget_musd") > 0,
                F.col("revenue_musd") / F.col("budget_musd")
            )
        )
        .groupBy("movie_type")
        .agg(
            F.avg("budget_musd").alias("mean_budget"),
            F.avg("revenue_musd").alias("mean_revenue"),
            F.avg("roi").alias("mean_roi")
        )
        .toPandas()
    )

    plot_data = data.melt(
        id_vars="movie_type",
        var_name="metric",
        value_name="value"
    )

    plt.figure(figsize=(10, 6))

    sns.barplot(
        data=plot_data,
        x="metric",
        y="value",
        hue="movie_type"
    )

    plt.title("Franchise vs. Standalone Movie Performance")
    plt.xlabel("Financial Metric")
    plt.ylabel("Average Value")
    plt.legend(title="Movie Type")
    plt.tight_layout()
    plt.savefig(REPORTS_DIR / "franchise_vs_standalone.png", dpi=300, bbox_inches="tight")
    plt.show()