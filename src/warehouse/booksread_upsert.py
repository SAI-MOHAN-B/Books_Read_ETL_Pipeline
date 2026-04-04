import configparser
from pathlib import Path

config = configparser.ConfigParser()
config.read_file(open(f"{Path(__file__).parents[0]}/warehouse_config.cfg"))

staging_schema = config.get("STAGING", 'SCHEMA')
warehouse_schema = config.get("WAREHOUSE", 'SCHEMA')

# ----------AUTHORS-----------------------------------

upsert_authors = f"""
BEGIN TRANSCATION;

DELETE FROM {1}.authors
using {0}.authors.author_id = {0}.authors.author_id;

INSERT INTO {1}.authors
SELECT * FROM {0}.authors

END TRANSCATION;
COMMIT;
""".format(staging_schema, warehouse_schema)

# -------------------------------------REVIEWS ------------------------------

upsert_review = """
BEGIN TRANSCATION;

DELETE FROM {1}.reviews
using {0}.reviews.review_id = {0}.reviews.review_id;

INSERT INTO {1}.reviews
SELECT * FROM {0}.reviews

END TRANSCATION;
COMMIT;
""".format(staging_schema, warehouse_schema)

# ------------------₹-------------------BOOKS------------------------------
upsert_books = """
BEGIN TRANSCATION;

DELETE FROM {1}.books
using {0}.books.book_id = {0}.books.book_id;
where {1}.books.book_id = {0}.books.book_id;

INSERT INTO {1}.books
SELECT * FROM {0}.books;
END TRANSCATION;
commit;
""".format(staging_schema, warehouse_schema)

#---------------------------------USERS------------------------------
upsert_users  = """
BEGIN TRANSCATION;

DELETE FROM {1}.users
using {0}.users.user_id = {0}.users.user_id;    

INSERT INTO {1}.users
SELECT * FROM {0}.users;
END TRANSCATION;
COMMIT;
""".format(staging_schema, warehouse_schema)
# ------------------------------BOOKS_READ------------------------------
upsert_queries = [upsert_authors, upsert_review, upsert_books, upsert_users]

