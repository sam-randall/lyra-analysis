

'''

Essentially, we want to be able to select the `import folder`.

Click Ingest -> that should refresh the analytics below.

There should be a dropdown selector to choose between the user, model and a specific user.

Then given that choice, there will be selectable analysis panels.

- Sentence Length is an expandable panel
    - when opened show percentiles
    - and a paginated view of the longest messages
    with the option to restrict by certain percentiles.

- Word Analysis:
    - list of top most frequently used words.

- Bigram Analysis
    - - list of top most frequently used bigrams.

'''

import streamlit as st
from lyra_analysis_app.ingest_transcripts_sqlite import ingest_data
from lyra_analysis_app.run_dashboard import get_phrase_frequencies, get_messages_sentence_length_percentiles_sqlite, get_messages_above_percentile_sqlite
import sqlite3
import pandas as pd

st.title("Lyra Analysis")

# Folder selector dropdown
# folder_options = ["Folder 1", "Folder 2", "Folder 3"]  # replace with dynamic folder listing if needed
selected_folder = "/Users/samrandall/Downloads/TRANSCRIPTS"

# Ingest button
if st.button("Ingest"):
    st.write(f"Running expensive ingestion script for folder: {selected_folder}...")
    # Call your expensive script here
    # e.g., run_ingest_script(selected_folder)
    ingest_data(selected_folder)
    # Make sure to use caching or background threads if it takes long
    st.success("Ingestion complete!")

# Speaker type dropdown
speaker_type = st.selectbox("Select Speaker", ["Lyra", "User", "Specific User"])

st.write(f"Selected Speaker: {speaker_type}")
st.write(f"Selected Folder: {selected_folder}")



# Exclusive expandable panels
panel = st.radio("Select Analysis Panel", ["Sentence Length", "Word Analysis", "Bigram Analysis"])

# -------------------------------
# Sentence Length Panel
# -------------------------------
if panel == "Sentence Length":
    st.subheader("Sentence Length Analysis")

    with sqlite3.connect(':memory') as conn:
        cursor = conn.cursor()
        col1, col2 = st.columns([1, 2])  # 1:2 ratio
        if speaker_type.lower() in ["lyra", "user"]:
            out = get_messages_sentence_length_percentiles_sqlite(conn, cursor, speaker_type.lower())
            with col1:
                st.write(out)


            with col2:
                st.subheader("Longest Messages.")
                top_messages: list[str] = get_messages_above_percentile_sqlite(
                    conn,
                    cursor,
                    speaker_type.lower(),
                    percentile=0.95
                )
                if top_messages:
                    for i, msg in enumerate(top_messages, start=1):
                        st.write(f"{i}. {msg}")
                else:
                    st.write("No messages found above the selected percentile.")
        else:
            st.write("Not yet implemented.")


# -------------------------------
# Word Analysis Panel
# -------------------------------
elif panel == "Word Analysis":
    st.subheader("Top Words")
    with sqlite3.connect(':memory') as conn:
        cursor = conn.cursor()
        phrase_freqs = get_phrase_frequencies(conn, cursor, speaker_type.lower(), limit = 50, num_words = 1)
        df = pd.DataFrame(phrase_freqs, columns=["Phrase", "Frequency"])
        st.table(df)
# -------------------------------
# Bigram Analysis Panel
# -------------------------------
elif panel == "Bigram Analysis":
    st.subheader("Top Bigrams")
    with sqlite3.connect(':memory') as conn:
        cursor = conn.cursor()
        phrase_freqs = get_phrase_frequencies(conn, cursor, speaker_type.lower(), limit = 50, num_words = 2)
        df = pd.DataFrame(phrase_freqs, columns=["Phrase", "Frequency"])
        st.table(df)
