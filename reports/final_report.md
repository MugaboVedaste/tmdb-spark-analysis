# TMDB Movie Data Analysis: Final Report

## 1. Introduction

This report summarizes the TMDB movie analysis built with Apache Spark (PySpark). The goal was to fetch movie data from the TMDB API, clean it, calculate performance KPIs, and present the findings with visualizations.

The analysis covers **18 movies**. The assignment listed 19 movie IDs, but ID `0` does not exist in TMDB (the API returns a 404 error), so it was skipped. All 18 movies are blockbusters that earned over $1.2 billion, so the findings describe this group only, not the whole film industry.

The full workflow is in `notebooks/tmdb_movie_analysis.ipynb`, and the reusable code is in the `src/` folder.

## 2. Methodology

### 2.1 Data Extraction

- `fetch_movie_data()` in `src/extraction.py` calls the TMDB endpoint `/movie/{id}` with `append_to_response=credits`, so each request returns the movie details together with its cast and crew.
- Failed requests are caught and reported instead of stopping the program.
- The raw responses are saved as JSON in `data/raw/` before any cleaning (the "bronze" layer).

### 2.2 Data Cleaning

The cleaning functions in `src/cleaning.py` run in this order:

1. Drop the irrelevant columns (`adult`, `imdb_id`, `original_title`, `video`, `homepage`, plus `softcore`, which TMDB returned and is not needed).
2. Extract the collection name, genres, spoken languages, production countries and production companies from the nested JSON. Multiple values are joined with `|`.
3. Extract the cast, cast size, director and crew size from the credits.
4. Convert data types (dates, numbers) and replace missing collection names with `No Collection`.
5. Inspect the extracted columns with value counts.
6. Replace zero values in budget, revenue and runtime with `NULL`.
7. Set `vote_average` to `NULL` where `vote_count` is 0.
8. Replace placeholder text in `overview` and `tagline` with `NULL`.
9. Remove duplicate movies and rows with an unknown `id` or `title`.
10. Keep only rows with at least 10 non-empty columns.
11. Keep only `Released` movies, then drop the `status` column.
12. Convert budget and revenue to millions of USD, then reorder the columns.
13. Save the result as Parquet in `data/processed/` (the "silver" layer).

Because this dataset contains only well-known, released blockbusters, steps 6 to 11 did not remove or change any rows. They are included so the pipeline stays correct if more movies are added later.

### 2.3 KPI Analysis

`src/kpi_analysis.py` ranks movies by revenue, budget, profit (revenue minus budget), ROI (revenue divided by budget), votes, rating and popularity. ROI rankings use only movies with a budget of at least $10M, and rating rankings use only movies with at least 10 votes. A PySpark UDF (`roi_udf`) is used to rank movies by ROI. The module also covers the two actor searches, the franchise vs standalone comparison, and the franchise and director rankings.

### 2.4 Visualization

`src/visualization.py` uses Matplotlib and Seaborn to create five charts, which are saved in this `reports/` folder.

## 3. Key Findings

### 3.1 Overall Numbers

| Measure | Value |
|---|---|
| Movies analysed | 18 |
| Total budget | $3,848 million |
| Total revenue | $30,597 million |
| Total profit | $26,749 million |
| Average rating | 7.40 / 10 |

### 3.2 Best and Worst Performers

| KPI | Movie | Value |
|---|---|---|
| Highest revenue | Avengers: Endgame | $2,942 million |
| Highest budget | Avengers: Endgame | $356 million |
| Highest profit | Avatar | $2,687 million |
| Lowest profit | Star Wars: The Last Jedi | $1,034 million |
| Highest ROI | Avatar | 12.34 |
| Lowest ROI | Star Wars: The Last Jedi | 4.45 |
| Most votes | The Avengers | 40,259 votes |
| Highest rated | Avengers: Endgame | 8.25 |
| Lowest rated | Jurassic World: Fallen Kingdom | 6.52 |
| Most popular | Avengers: Endgame | popularity 64.93 |

- Every movie made a profit, even the "lowest profit" movie. This happens because the dataset only contains blockbusters.
- Star Wars: The Last Jedi is the weakest performer on both profit and ROI. It had one of the highest budgets ($300 million, tied with Infinity War) but earned only 4.45 times its budget.
- Avatar earned less than Avengers: Endgame but had the best profit and ROI, because it cost $119 million less to make.

### 3.3 Franchise vs Standalone Movies

| Measure | Franchise (16 movies) | Standalone (2 movies) |
|---|---|---|
| Mean revenue | $1,692 million | $1,765 million |
| Median ROI | 7.71 | 7.91 |
| Mean budget | $218 million | $180 million |
| Mean popularity | 27.06 | 26.53 |
| Mean rating | 7.40 | 7.44 |

The two groups are very close. Franchise movies had bigger budgets, while standalone movies had slightly higher revenue, ROI and rating. However, only two movies (Titanic and Beauty and the Beast) are standalone, so we cannot conclude that standalone movies perform better.

### 3.4 Most Successful Franchises

| Franchise | Movies | Total revenue | Mean rating |
|---|---|---|---|
| The Avengers | 4 | $7,919 million | 7.96 |
| Star Wars | 2 | $3,403 million | 6.98 |
| Jurassic Park | 2 | $2,982 million | 6.61 |
| Avatar | 1 | $2,924 million | 7.61 |
| Frozen | 2 | $2,728 million | 7.23 |

The Avengers is by far the most successful franchise, with the highest total revenue, the highest total budget ($1,111 million) and the best mean rating of the multi-movie franchises. Harry Potter has the highest mean rating of any franchise (8.08) but only one movie in this dataset.

### 3.5 Most Successful Directors

| Director | Movies | Total revenue | Mean rating |
|---|---|---|---|
| James Cameron | 2 | $5,188 million | 7.76 |
| Joe Russo | 1 | $2,942 million | 8.25 |
| Joss Whedon | 2 | $2,924 million | 7.68 |
| Jennifer Lee | 2 | $2,728 million | 7.23 |

James Cameron (Avatar and Titanic) has the highest total revenue. Joe Russo appears with only one movie because of a data limitation explained in Section 5.

### 3.6 Advanced Searches

- **Science Fiction Action movies with Bruce Willis:** no results.
- **Movies with Uma Thurman directed by Quentin Tarantino:** no results.

Both searches are implemented correctly. They return empty tables because none of the 18 movies include these actors.

## 4. Insights from the Visualizations

1. **Revenue vs. Budget** (`revenue_vs_budget.png`): Higher budgets tend to go with higher revenue (correlation 0.58), but the link is weak. Avatar made almost as much as Avengers: Endgame with a budget $119 million lower.
2. **ROI by Genre** (`roi_by_genre.png`): Among genres with several movies, Romance, Fantasy and Drama have the highest median ROI, and Action has the lowest. Crime and Comedy each have only one movie, so their boxes are just a line.
3. **Popularity vs. Rating** (`popularity_vs_rating.png`): Popularity and rating rise together (correlation 0.84). The most popular movies are also among the best rated.
4. **Yearly Box Office Trends** (`yearly_box_office_trends.png`): Revenue is highest in 2015, 2018 and 2019, when several big franchise films were released. Budgets are also highest in those years. The line looks smooth between 1997 and 2009 only because no movies from the years in between were analysed.
5. **Franchise vs. Standalone** (`franchise_vs_standalone.png`): The bars confirm that revenue and budget are similar for both groups. The ROI bars are very short because ROI (about 8) is plotted on the same axis as amounts in the thousands.

## 5. Limitations

- **Small, biased sample:** 18 hand-picked blockbusters, so the results cannot be applied to all movies.
- **Only one director per movie:** `extract_director_and_crew_size` takes the first director listed. Avengers: Endgame and Avengers: Infinity War were co-directed by Joe and Anthony Russo, but Endgame is credited only to Joe and Infinity War only to Anthony, which splits their totals. Frozen and Frozen II were co-directed by Jennifer Lee and Chris Buck, but only Jennifer Lee is counted, so Chris Buck does not appear at all.
- **Very small groups:** the standalone group has two movies, and most directors and franchises have one or two, so differences between them should be treated carefully.
- **ROI is simple:** it is revenue divided by budget and ignores marketing, distribution and cinema revenue sharing.
- **Chart weaknesses:** the franchise chart mixes metrics with very different scales, and the yearly line connects years that have no data.
- **Missing actor data:** the two actor searches return no results for this dataset.

## 6. Conclusion

The pipeline successfully fetched, cleaned and analysed 18 TMDB movies with PySpark. Avengers: Endgame earned the most money, while Avatar was the most profitable and had the best ROI. The Avengers is the strongest franchise, and James Cameron is the director with the highest total revenue. Popular movies also tend to be highly rated. The franchise vs standalone comparison shows no clear difference, but the standalone sample is too small to be sure.

To make the results more reliable, the next step is to analyse many more movies, including low-budget and unsuccessful ones, and to include co-directors.
