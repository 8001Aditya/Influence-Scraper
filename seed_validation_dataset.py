import argparse
import os
import random
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import psycopg2
from dotenv import load_dotenv


@dataclass(frozen=True)
class Band:
    name: str
    min_followers: int
    max_followers: int
    count: int


INDIAN_FIRST_NAMES = [
    "Aarav",
    "Aditya",
    "Aisha",
    "Aman",
    "Ananya",
    "Arjun",
    "Diya",
    "Harsh",
    "Ishita",
    "Kabir",
    "Karan",
    "Kiara",
    "Meera",
    "Nisha",
    "Pooja",
    "Rahul",
    "Riya",
    "Rohan",
    "Sakshi",
    "Sneha",
    "Tanvi",
    "Vikram",
    "Yash",
    "Zoya",
]

INDIAN_LAST_NAMES = [
    "Agarwal",
    "Bhat",
    "Chauhan",
    "Das",
    "Gupta",
    "Iyer",
    "Jain",
    "Kapoor",
    "Khan",
    "Mehta",
    "Nair",
    "Patel",
    "Reddy",
    "Shah",
    "Sharma",
    "Singh",
    "Verma",
]

INDIAN_CITIES = [
    "Mumbai",
    "Delhi",
    "Bengaluru",
    "Hyderabad",
    "Pune",
    "Ahmedabad",
    "Chennai",
    "Kolkata",
    "Jaipur",
    "Lucknow",
]

CATEGORIES = ["fitness", "gaming", "tech"]
PLATFORMS = ["instagram", "youtube"]


def build_bands(total_rows: int) -> list[Band]:
    below_1_lakh = int(total_rows * 0.6)
    one_to_ten_lakh = int(total_rows * 0.3)
    one_crore_plus = total_rows - below_1_lakh - one_to_ten_lakh

    return [
        Band("below_1_lakh", 5_000, 99_999, below_1_lakh),
        Band("one_to_ten_lakh", 100_000, 999_999, one_to_ten_lakh),
        Band("one_crore_plus", 10_000_000, 25_000_000, one_crore_plus),
    ]


def pick_platform() -> str:
    return random.choices(PLATFORMS, weights=[0.55, 0.45], k=1)[0]


def build_username(
    first: str,
    last: str,
    category: str,
    platform: str,
    sequence_no: int,
    used: set[str],
) -> str:
    base = f"{first}_{last}_{category}_{sequence_no}".lower()
    if platform == "youtube":
        base = f"{base}_official"

    username = base
    suffix = 1
    while username in used:
        suffix += 1
        username = f"{base}_{suffix}"

    used.add(username)
    return username


def random_created_at() -> datetime:
    now = datetime.now(UTC).replace(tzinfo=None)
    days_ago = random.randint(1, 540)
    seconds_ago = random.randint(0, 86_399)
    return now - timedelta(days=days_ago, seconds=seconds_ago)


def random_posted_at(created_at: datetime) -> datetime:
    min_time = created_at
    max_time = datetime.now(UTC).replace(tzinfo=None)
    delta = max_time - min_time
    random_seconds = random.randint(0, max(1, int(delta.total_seconds())))
    return min_time + timedelta(seconds=random_seconds)


def connect_db():
    load_dotenv(".env")

    db_password = os.getenv("DB_PASSWORD", "")
    if db_password.strip().lower() == "your_db_password":
        raise ValueError("DB_PASSWORD is still set to placeholder value 'your_db_password' in .env")

    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME", "influencers_db"),
        user=os.getenv("DB_USER", "postgres"),
        password=db_password,
    )


def seed_dataset(total_rows: int) -> None:
    bands = build_bands(total_rows)
    used_usernames: set[str] = set()

    band_counter: Counter[str] = Counter()
    category_counter: Counter[str] = Counter()
    platform_counter: Counter[str] = Counter()

    conn = connect_db()
    cur = conn.cursor()

    try:
        # Reset validation dataset from all tables created in this project.
        cur.execute("TRUNCATE TABLE metrics, posts, influencers RESTART IDENTITY;")

        sequence_no = 1
        for band in bands:
            for _ in range(band.count):
                category = CATEGORIES[(sequence_no - 1) % len(CATEGORIES)]
                platform = pick_platform()
                first = random.choice(INDIAN_FIRST_NAMES)
                last = random.choice(INDIAN_LAST_NAMES)
                city = random.choice(INDIAN_CITIES)
                followers = random.randint(band.min_followers, band.max_followers)
                created_at = random_created_at()

                username = build_username(
                    first=first,
                    last=last,
                    category=category,
                    platform=platform,
                    sequence_no=sequence_no,
                    used=used_usernames,
                )
                fullname = f"{first} {last}"

                if platform == "instagram":
                    bio = f"Indian {category} creator from {city}. Daily reels and collaborations."
                    profile_url = f"https://www.instagram.com/{username}/"
                else:
                    bio = f"Indian {category} creator from {city}. Weekly videos and live sessions."
                    profile_url = f"https://www.youtube.com/@{username}"

                cur.execute(
                    """
                    INSERT INTO influencers (platform, username, fullname, followers, bio, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING id;
                    """,
                    (platform, username, fullname, followers, bio, created_at),
                )
                influencer_id = cur.fetchone()[0]

                post_count = random.randint(2, 5)
                likes_total = 0
                comments_total = 0

                for post_idx in range(1, post_count + 1):
                    likes = int(followers * random.uniform(0.008, 0.07))
                    comments = int(max(1, likes * random.uniform(0.04, 0.22)))
                    views = int(max(likes * random.uniform(6, 24), followers * random.uniform(0.2, 2.8)))
                    posted_at = random_posted_at(created_at)

                    if platform == "instagram":
                        post_url = f"{profile_url}p/{username}_post_{post_idx}_{influencer_id}/"
                    else:
                        post_url = f"https://www.youtube.com/watch?v={username}_{influencer_id}_{post_idx}"

                    cur.execute(
                        """
                        INSERT INTO posts (influencer_id, post_url, likes, comments, views, posted_at)
                        VALUES (%s, %s, %s, %s, %s, %s);
                        """,
                        (influencer_id, post_url, likes, comments, views, posted_at),
                    )

                    likes_total += likes
                    comments_total += comments

                avg_likes = int(likes_total / post_count)
                avg_comments = int(comments_total / post_count)
                engagement_rate = round(((avg_likes + avg_comments) / max(1, followers)) * 100, 2)

                cur.execute(
                    """
                    INSERT INTO metrics (influencer_id, engagement_rate, avg_likes, avg_comments, calculated_at)
                    VALUES (%s, %s, %s, %s, %s);
                    """,
                    (influencer_id, engagement_rate, avg_likes, avg_comments, datetime.now(UTC).replace(tzinfo=None)),
                )

                band_counter[band.name] += 1
                category_counter[category] += 1
                platform_counter[platform] += 1
                sequence_no += 1

        conn.commit()

        print("Seed completed.")
        print(f"Total influencers inserted: {total_rows}")
        print("Band split:", dict(band_counter))
        print("Category split:", dict(category_counter))
        print("Platform split:", dict(platform_counter))
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Truncate scraper tables and seed validation dataset for Indian fitness/gaming/tech influencers."
    )
    parser.add_argument("--rows", type=int, default=1000, help="Total influencers to seed (default: 1000)")
    parser.add_argument("--seed", type=int, default=42, help="Random seed (default: 42)")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.rows < 1:
        raise ValueError("--rows must be >= 1")

    random.seed(args.seed)
    seed_dataset(args.rows)


if __name__ == "__main__":
    main()
