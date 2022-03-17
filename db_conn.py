from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

from twitter_info import DATABASE_URL_WORKING


class PostgresConnection:
    def __init__(self):
        engine = create_engine(DATABASE_URL_WORKING)
        self.db = scoped_session(sessionmaker(bind=engine))

    def create_user_table(self):
        self.db.execute("""CREATE TABLE IF NOT EXISTS public."user" (
                                    id serial PRIMARY KEY,
                                    user_id BIGINT UNIQUE NOT NULL,
                                    tag VARCHAR ( 50 ),
                                    sended_message VARCHAR ( 280 ),
                                    sended BOOLEAN
                                    );""")
        self.db.commit()

    def get_all_user_already_in_tag(self, tag):
        query = "SELECT user_id FROM public.user WHERE tag=:tag"
        result = self.db.execute(query, {'tag': tag}).fetchall()
        return [x[0] for x in result]

    def insert_user_in_table(self, user_id, tag, sended_message="", sended=True):
        insert_dict = {"user_id": user_id, "tag": tag, "sended_message": sended_message, "sended": sended}

        try:
            self.db.execute(f"""INSERT INTO public."user"(user_id, tag, sended_message, sended) VALUES ({user_id}, '{tag}', '{sended_message}', {sended})""", insert_dict)
            self.db.commit()

        except Exception as ex:
            self.db.rollback()
            raise f"Erro ao inserir: {ex}"


        
        