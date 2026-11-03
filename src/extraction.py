
import requests


def fetch_movie_data(movie_ids, headers):
    """
    Fetch movie details and credits from the TMDB API.

    Args:
        movie_ids (list): List of TMDB movie IDs.
        headers (dict): API authorization headers.

    Returns:
        list: Successfully fetched movie data.
    """

    base_url = "https://api.themoviedb.org/3/movie"

    movie_data = []
    failed_movie_ids = []

    for movie_id in movie_ids:

        url = f"{base_url}/{movie_id}"

        params = {
            "append_to_response": "credits",
            "language": "en-US"
        }

        try:
            response = requests.get(
                url,
                headers=headers,
                params=params,
                timeout=30
            )

            response.raise_for_status()

            data = response.json()

            movie_data.append(data)

            print(f"Fetched: {movie_id} - {data.get('title')}")

        except requests.exceptions.RequestException as error:

            failed_movie_ids.append(movie_id)

            print(f"Failed to fetch movie ID {movie_id}: {error}")

    print("\nExtraction Summary")
    print("------------------")
    print(f"Successfully fetched: {len(movie_data)}")
    print(f"Failed: {len(failed_movie_ids)}")

    return movie_data
