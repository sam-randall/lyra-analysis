
from collections import Counter
from datetime import datetime
import nltk
import os
import re
import sqlite3
import pandas as pd
from tqdm import tqdm
import warnings

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



def main():
    '''Populates db (assumed already existing)
    with data from the transcripts located at `data_path`.
    '''

    data_path = "/Users/samrandall/Downloads/TRANSCRIPTS/"

    ingest_data(data_path)


if __name__ == "__main__":
  main()
