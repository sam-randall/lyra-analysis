
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
    print(df)

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


def main():
    print("hello world.")
    conn = psycopg2.connect(
        dbname="lyradb",
        user="postgres",
        password="password",
        host="localhost",
        port=5433
    )

    cur = conn.cursor()

    # What are the visualizations we need?

    # For each visualization, what data do we need?

    # (1) GET most frequent words/bigrams said by model with frequency.

    # (2) GET most frequent words/bigrams said by user with frequency.

    # (3a) GET percentiles for number of words per messages (DONE)
    # (3b) SHOW messages that have `num_words` exceeding a percentile (DONE)

    # (4) GET MESSAGES where a specific word or phrase was used.

    # Outstanding Issue
    # -> need to remove "uninteresting words", length of less


    # To support the contract, I need to add:
    # 1. number_of_words in a message. (DONE)
    # 2. previous_message_id: right now we can do (position - 1).


    # "User message length percentiles"
    out = get_messages_sentence_length_percentiles(conn, cur, "user")


    # Lyra message length percentiles
    out = get_messages_sentence_length_percentiles(conn, cur, "lyra")
    print(out)

    # Get user's messages above 0.95 percentile.
    longest_messages = get_messages_above_percentile(conn, cur, 'user', 0.95)

    phrase_freq = get_phrase_frequencies(conn, cur, 'lyra', limit = 50)

    print(phrase_freq)

    conn.close()
    cur.close()


if __name__ == "__main__":
    main()
