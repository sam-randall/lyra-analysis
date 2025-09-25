

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
import sqlite3
import pandas as pd


from collections import Counter
from datetime import datetime
import nltk
import os
import re
import sqlite3
import pandas as pd
from tqdm import tqdm
import warnings



from typing import Optional, List
import pandas as pd
import sqlite3


def get_messages_sentence_length_percentiles_sqlite(conn, cursor, speaker_type: Optional[str] = None):
    """
    Returns key percentiles (5th, 10th, 25th, 50th, 75th, 90th, 95th)
    of the 'word_count' column from the 'messages' table.
    """
    if speaker_type is not None:
        cursor.execute(
            "SELECT word_count FROM messages WHERE speaker_type = ?",
            (speaker_type,)
        )
    else:
        cursor.execute("SELECT word_count FROM messages")

    word_counts = [row[0] for row in cursor.fetchall()]

    if not word_counts:
        return {'p5': None, 'p10': None, 'p25': None, 'p50': None,
                'p75': None, 'p90': None, 'p95': None}

    s = pd.Series(word_counts)
    percentiles = s.quantile([0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95])

    return {
        'p5': percentiles[0.05],
        'p10': percentiles[0.10],
        'p25': percentiles[0.25],
        'p50': percentiles[0.50],
        'p75': percentiles[0.75],
        'p90': percentiles[0.90],
        'p95': percentiles[0.95]
    }


def get_messages_sentence_length_percentiles(conn, cursor, speaker_type: Optional[str] = None):
    """
    Returns key percentiles (10th, 25th, 50th, 75th, 90th)
    of the 'sentence_length' column from the 'messages' table.
    """
    if speaker_type is not None:
        query = f"""
        SELECT
            percentile_cont(0.05) WITHIN GROUP (ORDER BY word_count) AS p05,
            percentile_cont(0.10) WITHIN GROUP (ORDER BY word_count) AS p10,
            percentile_cont(0.25) WITHIN GROUP (ORDER BY word_count) AS p25,
            percentile_cont(0.50) WITHIN GROUP (ORDER BY word_count) AS p50,
            percentile_cont(0.75) WITHIN GROUP (ORDER BY word_count) AS p75,
            percentile_cont(0.90) WITHIN GROUP (ORDER BY word_count) AS p90,
            percentile_cont(0.95) WITHIN GROUP (ORDER BY word_count) AS p95
        FROM messages
        WHERE speaker_type = '{speaker_type}';
        """
    else:
        query = f"""
        SELECT
            percentile_cont(0.05) WITHIN GROUP (ORDER BY word_count) AS p05,
            percentile_cont(0.10) WITHIN GROUP (ORDER BY word_count) AS p10,
            percentile_cont(0.25) WITHIN GROUP (ORDER BY word_count) AS p25,
            percentile_cont(0.50) WITHIN GROUP (ORDER BY word_count) AS p50,
            percentile_cont(0.75) WITHIN GROUP (ORDER BY word_count) AS p75,
            percentile_cont(0.90) WITHIN GROUP (ORDER BY word_count) AS p90,
            percentile_cont(0.95) WITHIN GROUP (ORDER BY word_count) AS p95
        FROM messages;"""
    cursor.execute(query)
    result = cursor.fetchone()  # single row with all percentiles
    percentiles = {
        'p5': result[0],
        "p10": result[1],
        "p25": result[2],
        "p50": result[3],
        "p75": result[4],
        "p90": result[5],
        'p95': result[6]
    }
    return percentiles


def get_messages_above_percentile(conn, cursor, speaker_type, percentile=0.75):
    """
    Returns all messages for a given speaker_type whose word_count exceeds
    the specified percentile.

    :param conn: psycopg2 connection
    :param cursor: psycopg2 cursor
    :param speaker_type: string, e.g., 'lyra'
    :param percentile: float, 0 < percentile < 1
    :return: list of messages (rows)
    """
    # Step 1: Compute the percentile threshold
    cursor.execute("""
        SELECT percentile_cont(%s) WITHIN GROUP (ORDER BY word_count)
        FROM messages
        WHERE speaker_type = %s
    """, (percentile, speaker_type))
    threshold = cursor.fetchone()[0]

    # Step 2: Select messages above the threshold
    cursor.execute("""
        SELECT text
        FROM messages
        WHERE speaker_type = %s
          AND word_count > %s;
    """, (speaker_type, threshold))

    return cursor.fetchall()


def get_messages_above_percentile_sqlite(
    conn: sqlite3.Connection,
    cursor: sqlite3.Cursor,
    speaker_type: Optional[str] = None,
    percentile: float = 0.75
) -> List[str]:
    """
    Returns all messages for a given speaker_type whose word_count exceeds
    the specified percentile.

    :param conn: sqlite3 connection
    :param cursor: sqlite3 cursor
    :param speaker_type: string, e.g., 'lyra'
    :param percentile: float, 0 < percentile < 1
    :return: list of messages (strings)
    """
    # Step 1: Fetch word_counts and messages
    if speaker_type:
        cursor.execute(
            "SELECT word_count, text, tag FROM messages WHERE speaker_type = ? AND tag != '[Lyra Raw History]';",
            (speaker_type,)
        )
    else:
        cursor.execute("SELECT word_count, text, tag FROM messages WHERE tag != '[Lyra Raw History]';")

    rows = cursor.fetchall()
    if not rows:
        return []

    df = pd.DataFrame(rows, columns=["word_count", "text", "tag"])

    # Step 2: Compute threshold
    threshold = df["word_count"].quantile(percentile)

    # Step 3: Select messages above threshold
    messages_above = df[df["word_count"] > threshold]["text"].tolist()

    return messages_above

def get_phrase_frequencies(conn, cursor, speaker_type: str, limit: Optional[int] = 10, num_words: int = 2):
    query = f"""
    SELECT
        p.text AS phrase_text,
        COUNT(*) AS frequency
    FROM messages m
    JOIN phrases p ON m.id = p.message_id
    WHERE m.speaker_type = ? AND p.num_words = ?
    GROUP BY p.text
    ORDER BY frequency DESC
    LIMIT {limit};
    """
    cursor.execute(query, (speaker_type, num_words))
    return cursor.fetchall()

def get_phrases_from_message(message: str):
    words = nltk.word_tokenize(message)
    bigrm = nltk.bigrams(words)
    words = [{'text': text, 'num_words': 1} for text in words]
    bigrams = [{'text': f'{text[0]} {text[1]}', 'num_words': 2} for text in bigrm]
    return words + bigrams

def read_file(file_path: str):

    match = re.search(r"(\d{8}-\d{6})", file_path)
    dt = None
    if match:
        ts_str = match.group(1)
        dt = datetime.strptime(ts_str, "%Y%m%d-%H%M%S")

    transcript = {
        'filepath': file_path,
        'timestamp': dt
    }

    # Parse Messages from transcript.
    messages = []
    all_phrases = []

    n_lines_successfully_processed = 0
    total_lines_processed = 0

    with open(file_path, encoding="utf-8", errors="replace") as f:
        text = f.read()
        parts = re.split(r"(\n\[[^\]]+\])", text)

        # Recombine: each chunk = marker + following text
        chunks = []
        for i in range(1, len(parts), 2):  # markers are odd indices, content is even
            marker = parts[i].strip()  # e.g. "[123]"
            content = parts[i+1].strip() if i+1 < len(parts) else ""
            message_data = {
                'tag': marker,
                'text': content
            }

            line_number = i // 2

            message_data['position'] = line_number

            if 'text' in message_data:
                phrases = get_phrases_from_message(message_data["text"])

                for phrase in phrases:
                    phrase['filepath'] = transcript['filepath']
                    phrase['message_id'] = line_number
                all_phrases.append(phrases)

            messages.append(message_data)
            n_lines_successfully_processed += 1
            total_lines_processed += 1


    transcript['message_count'] = len(messages)

    return (transcript, messages, all_phrases), (n_lines_successfully_processed, total_lines_processed)

def delete_tables(conn, cursor):

    commands = [
        "DROP TABLE IF EXISTS transcripts;",
        "DROP TABLE IF EXISTS messages;",
        "DROP TABLE IF EXISTS phrases;"

    ]

    for command in commands:
        cursor.execute(command)
    conn.commit()

def setup_db(conn, cursor):
    commands = [
        """CREATE TABLE IF NOT EXISTS transcripts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filepath TEXT UNIQUE NOT NULL,
            timestamp TIMESTAMP,
            message_count INTEGER
        );""",
        """
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            transcript_id INTEGER NOT NULL REFERENCES transcripts(id) ON DELETE CASCADE,
            tag TEXT,
            speaker_type TEXT NOT NULL CHECK(speaker_type IN ('lyra', 'user', 'unknown')),
            position INTEGER,
            previous_message_id INTEGER,
            name TEXT,
            role TEXT,
            text TEXT,
            word_count INTEGER,
            UNIQUE (transcript_id, position)
        );
        """,
        """CREATE TABLE IF NOT EXISTS phrases (
            text TEXT,
            num_words INTEGER,
            filepath TEXT,
            message_id INTEGER NOT NULL REFERENCES messages(id)
        );"""

    ]
    for command in commands:
        cursor.execute(command)
    conn.commit()



def get_word_count(text: str):
    return len(text.split())

def get_speaker_type(tag: str):
    if "LLM" in tag:
        return "lyra"
    elif "Lyra" in tag:
        return "lyra"
    elif "STT" in tag:
        return "user"
    else:
        return "unknown"


def write_to_db(conn, cursor, transcript, messages, phrases):
    """
    Inserts a transcript, its messages, and phrases into the database.

    Parameters:
    - transcript: dict with at least {"filepath": ..., "timestamp": ..., "message_count": ...}
    - messages: list of dicts, each with keys {"tag", "position", "name", "role", "text"}
    - phrases: list of lists of dicts, each with keys {"text", "num_words", "filepath"} corresponding to messages
    """

    # 1️⃣ Insert transcript
    cursor.execute(
        "INSERT INTO transcripts (filepath, timestamp, message_count) VALUES (?, ?, ?)",
        (transcript["filepath"], transcript["timestamp"], transcript["message_count"])
    )
    transcript_id = cursor.lastrowid  # get the SQLite ID of the inserted transcript

    # 2️⃣ Insert messages and track their IDs
    message_ids = []
    for msg in messages:
        cursor.execute(
            """
            INSERT INTO messages
            (transcript_id, tag, speaker_type, position, name, role, text, word_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                transcript_id,
                msg.get("tag"),
                get_speaker_type(msg.get("tag")),
                msg.get("position"),
                msg.get("name"),
                msg.get("role"),
                msg.get("text"),
                get_word_count(msg.get("text"))
            )
        )
        message_ids.append(cursor.lastrowid)


    # 3️⃣ Map phrases to the correct message IDs
    all_phrases = []
    for m_id, phrase_list in zip(message_ids, phrases):
        if phrase_list:  # skip empty lists
            all_phrases.extend([
                (phrase['text'], phrase['num_words'], phrase['filepath'], m_id)
                for phrase in phrase_list
            ])

    # 4️⃣ Bulk insert phrases
    if all_phrases:
        for phrase in all_phrases:
            cursor.execute(
                """
                INSERT INTO phrases (text, num_words, filepath, message_id)
                VALUES (?, ?, ?, ?)
                """,
                (phrase[0], phrase[1], phrase[2], phrase[3])
            )

    # 5️⃣ Commit all changes at once
    # conn.commit()



def ingest_data(data_path: str):

    files = os.listdir(data_path)
    txt_files = [file for file in files if file.endswith('.txt')]
    with sqlite3.connect(':memory') as conn:

        cur = conn.cursor()

        # Temporary to get schema correct.
        delete_tables(conn, cur)

        setup_db(conn, cur)

        for txt_file in tqdm(txt_files):

            transcript_path = os.path.join(data_path, txt_file)

            (transcript, messages, statistics), (n_lines, total_lines) = read_file(transcript_path)

            write_to_db(conn, cur, transcript, messages, statistics)

        conn.commit()

        # 2. Connect to a file database
        disk_conn = sqlite3.connect("lyra_transcripts.db")

        # 3. Backup in-memory database to file
        conn.backup(disk_conn)

        disk_conn.close()



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
