# TMDB Movie Data Analysis Using PySpark

## 1. Project Overview

This project analyzes movie data collected from **The Movie Database (TMDB) API** using Apache Spark (PySpark).

The main objective is to explore movie performance through financial metrics, audience ratings, popularity, genres, directors, and movie franchises.

The project demonstrates a modular data processing workflow, from API data extraction and cleaning to analysis and visualization.

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
| Python | Main programming language |
| Apache Spark (PySpark) | Distributed data processing |
| Pandas | Data handling for visualization |
| Matplotlib | Data visualization |
| Seaborn | Statistical visualizations |
| Jupyter Notebook | Interactive analysis |
| TMDB API | Movie data source |
| python-dotenv | Environment variable management |

## 4. Project Structure

```text
tmdb-spark-analysis/
│
├── data/
│   ├── raw/
│   └── processed/
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
│
├── .env
├── .gitignore
├── requirements.txt
└── README.md
```

### Folder Description

- **data/raw/**: Stores raw movie data collected from the API.
- **data/processed/**: Stores cleaned and processed datasets.
- **notebooks/**: Contains the Jupyter Notebook documenting the analysis workflow.
- **src/**: Contains reusable Python functions organized into separate modules.
- **reports/**: Stores generated analysis reports and outputs.
- **.env**: Stores API credentials and other environment variables.
- **requirements.txt**: Lists the Python dependencies.

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

Movie details and credits are retrieved through the API.

**Note:** The current analysis uses a limited selection of movie IDs rather than the entire TMDB database.

## 6. Project Workflow

The project follows these main stages:

### Stage 1: Data Extraction

Movie information is retrieved from the TMDB API using Python requests.

The extraction logic is implemented in `src/extraction.py`.

### Stage 2: Data Cleaning and Transformation

The extracted data is prepared for analysis using PySpark.

This stage includes:

- Handling missing values.
- Cleaning and transforming columns.
- Extracting relevant movie information.
- Preparing numerical fields for analysis.
- Converting budget and revenue into millions of USD.

The cleaning logic is implemented in `src/cleaning.py`.

### Stage 3: KPI Analysis

PySpark is used to calculate key performance indicators and answer analytical questions.

The analysis includes:

- Highest and lowest revenue movies.
- Highest and lowest production budgets.
- Most profitable movies.
- Highest and lowest ROI.
- Most popular and most voted movies.
- Highest and lowest rated movies.
- Advanced searches involving actors, genres, and directors.
- Franchise versus standalone movie comparisons.
- Most successful movie franchises.
- Most successful directors.

A custom PySpark User-Defined Function (UDF) is also implemented to calculate and rank movie ROI.

The analysis functions are stored in `src/kpi_analysis.py`.

### Stage 4: Data Visualization

Matplotlib and Seaborn are used to present the findings visually.

Five visualizations are implemented:

1. **Revenue vs. Budget Trends:** Examines the relationship between movie production budgets and box office revenue.
2. **ROI Distribution by Genre:** Compares the distribution of financial returns across movie genres.
3. **Popularity vs. Rating:** Explores the relationship between movie popularity and audience ratings.
4. **Yearly Trends in Box Office Performance:** Examines changes in total movie budgets and revenue over release years.
5. **Franchise vs. Standalone Success:** Compares the average financial performance of franchise and standalone movies.

The visualization functions are stored in `src/visualization.py`.

## 7. Installation and Setup

### Prerequisites

- Python 3.11
- Java Development Kit (JDK) compatible with your Spark version
- Apache Spark / PySpark
- Visual Studio Code
- Jupyter Notebook

### Step 1: Clone the Repository

```bash
git clone <YOUR_REPOSITORY_URL>
cd tmdb-spark-analysis
```

### Step 2: Create a Virtual Environment

```bash
py -3.11 -m venv .venv
```

Activate it on Windows:

```bash
.venv\Scripts\activate
```

### Step 3: Install Dependencies

```bash
python -m pip install --upgrade pip
pip install -r requirements.txt
```

### Step 4: Configure the TMDB API

Create a `.env` file in the project root and add your TMDB API access token using the environment variable expected by `src/extraction.py`.

Keep your API credentials private. Never commit the `.env` file to GitHub.

### Step 5: Register the Jupyter Kernel

```bash
python -m pip install ipykernel
python -m ipykernel install --user --name tmdb-spark --display-name "Python 3.11 (TMDB Spark)"
```

## 8. Running the Project

1. Open the project in Visual Studio Code.
2. Activate the project virtual environment.
3. Open `notebooks/tmdb_movie_analysis.ipynb`.
4. Select the registered Python kernel.
5. Run the notebook cells sequentially, starting with Spark initialization and data extraction.
6. Follow the notebook through data cleaning, KPI analysis, and visualization.

The notebook provides explanations alongside the code for each stage of the workflow.

## 9. Key Findings

The analysis identifies several notable results within the selected dataset:

- **Highest revenue:** Avengers: Endgame.
- **Highest profit:** Avatar.
- **Highest production budget:** Avengers: Endgame.
- **Highest ROI among movies with a budget of at least $10 million:** Avatar.
- **Highest-grossing franchise:** Avengers Collection.
- **Highest total revenue by director:** James Cameron.

These findings are specific to the movies included in the current dataset.

## 10. Limitations

- The project currently analyzes a small selection of movies, so its findings are not representative of the entire movie industry.
- Some actor-based searches return no results because the selected dataset does not contain the requested actors.
- ROI is calculated as revenue divided by production budget. It does not account for marketing, distribution, or other costs.
- Movie budgets and revenues are based on the information provided by TMDB.

## 11. Future Improvements

- Expand the dataset to include more movies from TMDB.
- Automate data extraction and refresh.
- Introduce additional analytical metrics.
- Improve visualization and reporting.
- Explore advanced Spark optimization techniques.
- Build an interactive dashboard for exploring movie performance.

## 12. Author

**Vedaste MUGABO**

Computer Science Student | Data Engineering

GitHub: [Your GitHub Profile](<YOUR_GITHUB_PROFILE_URL>)

Portfolio: [vedaste.tech](https://vedaste.tech)