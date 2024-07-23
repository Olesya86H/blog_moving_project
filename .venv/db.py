from sqlalchemy import create_engine
import Config

engine = create_engine(
    url = Config.DB_CONN_URL,
    echo = Config.DB_ECHO,)