# TMDB Movie Data Analysis Using PySpark

## 1. Project Overview

This project analyzes movie data collected from **The Movie Database (TMDB) API** using Apache Spark (PySpark).

The main objective is to explore movie performance through financial metrics, audience ratings, popularity, genres, directors, and movie franchises.

The project demonstrates a modular data processing workflow, from API data extraction and cleaning to analysis and visualization.

A written summary of the methodology, results and conclusions is available in [reports/final_report.md](reports/final_report.md).

## 2. Project Objectives

- Extract movie details and cast information using the TMDB API.
- Process and clean movie data using PySpark.
- Perform exploratory data analysis and calculate key performance indicators (KPIs).
- Identify top-performing movies, directors, and franchises.
- Compare franchise movies with standalone movies.
- Create visualizations to communicate findings.

## 3. Technologies Used

| Technology | Purpose |
|---|---|
| Python 3.11 | Main programming language |
| Apache Spark (PySpark 4.0.1) | Distributed data processing |
| Pandas | Data handling for visualization |
| Matplotlib | Data visualization |
| Seaborn | Statistical visualizations (built on Matplotlib) |
| Jupyter Notebook | Interactive analysis |
| TMDB API | Movie data source |
| Requests | Calling the TMDB API |
| python-dotenv | Environment variable management |

## 4. Project Structure

```text
tmdb-spark-analysis/
│
├── data/
│   ├── raw/                  # Raw API data (created by the notebook, not committed)
│   └── processed/            # Cleaned data saved as Parquet
│
├── notebooks/
│   └── tmdb_movie_analysis.ipynb
│
├── src/
│   ├── __init__.py
│   ├── extraction.py
│   ├── cleaning.py
│   ├── kpi_analysis.py
│   └── visualization.py
│
├── reports/
│   ├── final_report.md
│   └── *.png                 # The five visualizations
│
├── .env                      # Your API token (you create it, not committed)
├── .gitignore
├── requirements.txt
├── requirements-pyspark313.txt
└── README.md
```

### 4.1 Folder and File Description

- **data/raw/**: Raw movie data collected from the API. It is ignored by Git and is recreated when you run the notebook.
- **data/processed/**: Cleaned and processed dataset saved in Parquet format.
- **notebooks/**: The Jupyter Notebook that documents the full workflow step by step.
- **src/**: Reusable Python functions, one module per stage.
- **reports/**: The final report and the saved visualizations.
- **.env**: Stores your TMDB API token. Create it yourself and never commit it.
- **requirements.txt**: Python dependencies for the project (use this one).
- **requirements-pyspark313.txt**: A full list of every package from a separate Python 3.13 environment. It does not include Seaborn, so use `requirements.txt` instead.

The notebook also creates a `src.zip` file (a zip of the `src/` folder) so Spark can use the custom UDF. It is generated automatically and ignored by Git.

## 5. Data Source

The project uses the TMDB API to retrieve movie information.

The extracted information includes:

- Movie titles and IDs.
- Genres and release dates.
- Production budgets and box office revenue.
- Popularity and audience ratings.
- Movie runtime.
- Cast and director information.
- Movie collection information.

Movie details and credits are retrieved in a single request per movie (`/movie/{id}` with `append_to_response=credits`).

**Note:** The analysis uses 19 selected movie IDs rather than the entire TMDB database. ID `0` does not exist in TMDB and returns a 404 error, so **18 movies** are analysed.

## 6. Project Workflow

The project follows four main stages.

### 6.1 Stage 1: Data Extraction

Movie information is retrieved from the TMDB API using Python `requests`. Failed requests are reported and skipped. The raw responses are saved as JSON in `data/raw/` before any cleaning.

The extraction logic is implemented in `src/extraction.py`.

### 6.2 Stage 2: Data Cleaning and Transformation

The raw data is loaded into a Spark DataFrame and cleaned in these steps:

1. Drop irrelevant columns.
2. Extract the collection name, genres, spoken languages, production countries and production companies (multiple values joined with `|`).
3. Extract the cast, cast size, director and crew size.
4. Convert data types and replace missing collection names with `No Collection`.
5. Inspect the extracted columns with value counts.
6. Replace unrealistic zero values in budget, revenue and runtime with `NULL`.
7. Set the rating to `NULL` for movies with zero votes.
8. Replace placeholder text in `overview` and `tagline` with `NULL`.
9. Remove duplicate movies and rows with an unknown `id` or `title`.
10. Keep only rows with at least 10 non-empty columns.
11. Keep only `Released` movies and drop the `status` column.
12. Convert budget and revenue to millions of USD and reorder the columns.
13. Save the result as Parquet in `data/processed/`.

The cleaning logic is implemented in `src/cleaning.py`.

### 6.3 Stage 3: KPI Analysis

PySpark is used to calculate key performance indicators and answer analytical questions.

The analysis includes:

- Highest and lowest revenue movies.
- Highest and lowest production budgets.
- Most profitable and least profitable movies.
- Highest and lowest ROI (only movies with a budget of at least $10 million).
- Most popular and most voted movies.
- Highest and lowest rated movies (only movies with at least 10 votes).
- Advanced searches involving actors, genres, and directors.
- Franchise versus standalone movie comparisons.
- Most successful movie franchises.
- Most successful directors.

A custom PySpark User-Defined Function (UDF) is also implemented to calculate and rank movie ROI.

The analysis functions are stored in `src/kpi_analysis.py`.

### 6.4 Stage 4: Data Visualization

Matplotlib and Seaborn are used to present the findings visually. Each chart is displayed in the notebook and saved to `reports/`.

Five visualizations are implemented:

1. **Revenue vs. Budget Trends:** Examines the relationship between movie production budgets and box office revenue.
2. **ROI Distribution by Genre:** Compares the distribution of financial returns across movie genres.
3. **Popularity vs. Rating:** Explores the relationship between movie popularity and audience ratings.
4. **Yearly Trends in Box Office Performance:** Examines changes in total movie budgets and revenue over release years.
5. **Franchise vs. Standalone Success:** Compares the average financial performance of franchise and standalone movies.

The visualization functions are stored in `src/visualization.py`.

## 7. Installation and Setup

### 7.1 Prerequisites

- Python 3.11
- Java Development Kit (JDK) 17 or 21 (required by Spark 4)
- Visual Studio Code with the Jupyter extension (or Jupyter Notebook)
- A free TMDB account to get an API token
- **Windows only:** the Hadoop `winutils` files (see Section 7.6)

### 7.2 Clone the Repository

```bash
git clone https://github.com/MugaboVedaste/tmdb-spark-analysis.git
cd tmdb-spark-analysis
```

### 7.3 Create a Virtual Environment

```bash
py -3.11 -m venv .venv
```

Activate it on Windows:

```bash
.venv\Scripts\activate
```

### 7.4 Install Dependencies

```bash
python -m pip install --upgrade pip
pip install -r requirements.txt
```

### 7.5 Configure the TMDB API

1. Create a free account at [themoviedb.org](https://www.themoviedb.org/).
2. Open **Settings → API** and copy your **API Read Access Token** (the long token, not the short API key).
3. Create a file named `.env` in the project root with this line:

```text
TMDB_READ_ACCESS_TOKEN=your_read_access_token_here
```

The notebook reads this variable in Section 5 (API Authentication) and sends it as a Bearer token. Keep your token private and never commit the `.env` file. It is already listed in `.gitignore`.

### 7.6 Windows Only: Configure Hadoop

On Windows, Spark needs `winutils.exe` and `hadoop.dll` to write files.

1. Download the Hadoop `winutils` files that match your Hadoop version.
2. Put them in a folder such as `D:\hadoop\bin`.
3. In the notebook, Section 3 (Spark Session Initialization) sets `HADOOP_HOME` to `D:\hadoop`. If you used a different folder, change the path in that cell.

On Linux or macOS you can skip this step and remove the Hadoop lines from that cell.

### 7.7 Register the Jupyter Kernel

```bash
python -m ipykernel install --user --name tmdb-spark --display-name "Python 3.11 (TMDB Spark)"
```

## 8. Running the Project

1. Open the project in Visual Studio Code.
2. Activate the project virtual environment.
3. Open `notebooks/tmdb_movie_analysis.ipynb`.
4. Select the registered Python kernel.
5. Run the notebook cells in order, starting with Spark initialization and data extraction.

Running the notebook calls the TMDB API again, recreates `data/raw/` and overwrites `data/processed/`.

The notebook provides explanations alongside the code for each stage. Its sections are:

| Notebook sections | Content |
|---|---|
| 1 to 3 | Introduction and Spark setup |
| 4 to 8 | API extraction and saving the raw data |
| 9 to 13 | Data cleaning and saving the processed data |
| 14 | KPI analysis and searches |
| 15 | Visualizations |

## 9. Key Findings

The analysis identifies several notable results within the selected dataset of 18 movies:

- **Highest revenue:** Avengers: Endgame ($2,942 million).
- **Highest budget:** Avengers: Endgame ($356 million).
- **Highest profit:** Avatar ($2,687 million).
- **Highest ROI (budget of at least $10 million):** Avatar (12.34).
- **Lowest ROI:** Star Wars: The Last Jedi (4.45).
- **Highest-grossing franchise:** The Avengers Collection ($7,919 million from 4 movies).
- **Highest total revenue by director:** James Cameron ($5,188 million from 2 movies).
- **Franchise vs standalone:** very similar results, but only 2 standalone movies exist in the dataset.

These findings are specific to the movies included in the current dataset. See [reports/final_report.md](reports/final_report.md) for the full results and discussion.

## 10. Limitations

- The project analyzes only 18 hand-picked blockbusters, so its findings are not representative of the entire movie industry.
- The Bruce Willis and Uma Thurman searches return no results because none of the selected movies include these actors.
- Only the first director listed is kept for each movie, so co-directors (for example the Russo brothers) are not counted together.
- ROI is calculated as revenue divided by production budget. It does not account for marketing, distribution, or other costs.
- Movie budgets and revenues are based on the information provided by TMDB.
- The cleaning rules for zero values, placeholders and unreleased movies are implemented, but they do not change this dataset because it only contains well-known released movies.

## 11. Future Improvements

- Expand the dataset to include more movies from TMDB, including low-budget and unsuccessful ones.
- Keep all directors of a movie instead of only the first.
- Improve two charts: show ROI on its own axis in the franchise chart, and avoid joining years that have no data in the yearly chart.
- Automate data extraction and refresh.
- Introduce additional analytical metrics.
- Explore advanced Spark optimization techniques.
- Build an interactive dashboard for exploring movie performance.

## 12. Author

**Vedaste MUGABO**

Computer Science Student | Data Engineering

GitHub: [MugaboVedaste](https://github.com/MugaboVedaste)

Portfolio: [vedaste.tech](https://vedaste.tech)
