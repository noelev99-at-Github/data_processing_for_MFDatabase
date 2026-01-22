import pandas as pd
import requests
import time
import os

from dotenv import load_dotenv

load_dotenv()

def apimoviecall(title, year):
    try:
        print(f"Sending Plot And Poster Data Request to OMDb for {title} {year}")

        apikey = os.getenv("OMDB_API_KEY")
        plot = "short"

        params = {
            "t": title,
            "apikey": apikey,
            "plot": plot,
            "y": year,
        }

        response = requests.get("https://www.omdbapi.com/", params=params)
        data = response.json()

        return data

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return {"Plot": "NONE FOUND", "Poster": "NONE FOUND"}

# Load your CSV
df = pd.read_csv("moviefeelsexceldatabase_cleaned_ALLNUM.csv")

# List of all mood columns
mood_columns = [
    "Love · Romance · Family · Community · Belonging · Home",
    "Happy · Playful · Bright · Feel-good · Carefree",
    "Hopeful · Healing · Optimistic · Reassuring",
    "Excited · Adventurous · Fun · Escapist",
    "Reflective · Introspective · Contemplative About Life",
    "Calm · Peaceful · Relaxed · Soft · Gentle",
    "Curious · Engaged · Intrigued · Mentally Active",
    "Intense · Emotional · Cathartic · Bittersweet",
    "Lonely · Isolated · Unseen · Longing",
    "Angry · Frustrated · Irritated · Stressed",
    "Hopeless · Sad · Heartbroken · Melancholy",
    "Scared · Anxious · Uneasy · Tense · Nervous"
]

for row in df.index:
    print("________________________________________________________________________________________________________________\n")

    try:
        titleForAPI = df.loc[row, "Movie"]
        yearoftitleForAPI = df.loc[row, "Year"]

        # Call OMDb API
        dataFromAPI = apimoviecall(titleForAPI, yearoftitleForAPI)
        Plot = dataFromAPI.get("Plot", "NONE FOUND")
        Poster = dataFromAPI.get("Poster", "NONE FOUND")

        print(f"\nRow Number: {row} - Initializing sending data to database moviefeels\n")
        print(df.loc[row], "\n")  

        # Build moods dictionary dynamically
        moods_dict = {}
        for col in mood_columns:
            value = df.loc[row, col]
            if value != 0:  # Only include non-zero values
                moods_dict[col] = value

        # Build data payload
        data = {
            "title": df.loc[row, "Movie"],
            "year": int(df.loc[row, "Year"]),
            "synopsis": Plot,
            "image_url": Poster,
            "storyline": df.loc[row, "Storyline"],
            "moods": moods_dict
        }

        # Send the POST request
        url = "http://localhost:8000/api/movies"
        response = requests.post(url, json=data)
        print(f"\nRow Number: {row} \n")
        print(response.json())

    except Exception as e:
        print(f"Failed to process or send row {row}: {e}")

    print("________________________________________________________________________________________________________________\n")
    time.sleep(0.2)

print("FINALLY ALL MOVIES SUCCESSFULLY SENT TO MOVIE FEELS DATABASE")
