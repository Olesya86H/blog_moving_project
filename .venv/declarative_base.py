from itertools import zip_longest
from typing import Sequence

from sqlalchemy import Table
from sqlalchemy import ForeignKey
from sqlalchemy import String
from sqlalchemy import Integer
from sqlalchemy import DateTime
from sqlalchemy import Boolean
from sqlalchemy import Float
from sqlalchemy import Column
from sqlalchemy import MetaData
from sqlalchemy import insert
from sqlalchemy import select
from sqlalchemy import update
from sqlalchemy import func
from sqlalchemy import text
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Session
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship
from datetime import datetime

from db import engine

import pytest

metadata = MetaData()

class Base(DeclarativeBase):
    pass

#Блог о моем текущем переезде
#справочник пользователей, которые могут писать посты в блоге
s_users_table = Table(
    "s_users",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("username", String(32), unique=True, nullable=False),
    Column("email", String(255), unique=True, nullable=True),
)

#таблица написанных постов в блоге
t_posts_table = Table(
    "t_posts",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("user_id", Integer, nullable=False),
    Column("description", String(255), unique=True, nullable=True),
)

#справочник тегов
s_tags_table = Table(
    "s_tags",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("description", String(255), unique=True, nullable=True),
)

#справочник агентов, с которыми заключаются договора на услуги
s_agents = Table(
    "s_agents",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("name", String(255), unique=True, nullable=True),
)

#справочник услуг
s_agents_conn_types = Table(
    "s_agents_conn_types",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("description", String(255), unique=True, nullable=True),
    )

#таблица взаимодействий с агентами (история)
t_agents_conn = Table(
    "t_agents_conn",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("agent_id", Integer, nullable=False),
    Column("conn_type_id", Integer, nullable=False),
    Column("connection_date", DateTime, nullable=True),
    Column("done", Boolean, nullable=True),
    Column("note", String(255), nullable=True),
    Column("costs", Float, nullable=True),
)

class User(Base):
    __tablename__ = "s_users"
    id: Mapped[int] = mapped_column(primary_key=True)
    username: Mapped[str] = mapped_column(String(32), unique=True)
    email: Mapped[str | None] = mapped_column(String(255), unique=True)
    posts: Mapped[list["Post"]] = relationship(back_populates="user")

    def __str__(self):
        return (
            f"{self.__class__.__name__}("
            f"id={self.id!r}, "
            f"username={self.username!r}, "
            f"email={self.email!r}"
            ")"
        )

    def __repr__(self):
        return str(self)

class Post(Base):
    __tablename__ = "t_posts"
    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("s_users.id"))
    description: Mapped[str | None] = mapped_column(String(255))
    user: Mapped["User"] = relationship(back_populates="posts")
    def __str__(self):
        return (
            f"{self.__class__.__name__}("
            f"id={self.id!r}, "
            f"user_id={self.user_id!r}, "
            f"description={self.description!r}"
            ")"
        )

    def __repr__(self):
        return str(self)

class Tag(Base):
    __tablename__ = "s_tags"
    id: Mapped[int] = mapped_column(primary_key=True)
    description: Mapped[str | None] = mapped_column(String(255))

    def __str__(self):
        return (
            f"{self.__class__.__name__}("
            f"id={self.id!r}, "
            f"description={self.description!r}"
            ")"
        )

    def __repr__(self):
        return str(self)

class Agent(Base):
    __tablename__ = "s_agents"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str | None] = mapped_column(String(255))
    #conns_ag: Mapped[list["Agent_Conn"]] = relationship(back_populates="conn_ag")

    def __str__(self):
        return (
            f"{self.__class__.__name__}("
            f"id={self.id!r}, "
            f"name={self.name!r}"
            ")"
        )

    def __repr__(self):
        return str(self)

class Agent_Conn_Type(Base):
    __tablename__ = "s_agents_conn_types"
    id: Mapped[int] = mapped_column(primary_key=True)
    description: Mapped[str | None] = mapped_column(String(255))
    #conns_types: Mapped[list["Agent_Conn"]] = relationship(back_populates="conn_type")

    def __str__(self):
        return (
            f"{self.__class__.__name__}("
            f"id={self.id!r}, "
            f"description={self.description!r}"
            ")"
        )

    def __repr__(self):
        return str(self)

class Agent_Conn(Base):
    __tablename__ = "t_agents_conn"
    id: Mapped[int] = mapped_column(primary_key=True)
    agent_id: Mapped[int] = mapped_column(Integer)#ForeignKey("s_agents.id"))
    conn_type_id: Mapped[int] = mapped_column(Integer)#ForeignKey("s_agents_conn_types.id"))
    connection_date: Mapped[DateTime] = mapped_column(DateTime)
    done: Mapped[bool] = mapped_column(Boolean)
    note: Mapped[str | None] = mapped_column(String(255))
    costs: Mapped[float] =mapped_column(Float)
    #conn_ag: Mapped["Agent"] = relationship(back_populates="conns_ag")
    #conn_type: Mapped["Agent_Conn_Type"] = relationship(back_populates="conns_type")

    def __str__(self):
        return (
            f"{self.__class__.__name__}("
            f"id={self.id!r}, "
            f"agent_id={self.agent_id!r}, "
            f"conn_type_id={self.conn_type_id!r}, "
            f"connection_date={self.connection_date!r}, "
            f"done={self.done!r}, "
            f"note={self.note!r}, "
            f"costs={self.costs!r}"
            ")"
        )

    def __repr__(self):
        return str(self)

def create_table():
    print("tables:", metadata.tables)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)


def insert_rows(session: Session):
    user_admin = User(
        username="admin",
        email="admin@admin.ru",
    )
    session.add(user_admin)

    users = [
        User(username=username, email=email)
        for username, email in zip_longest(
            ["Olesya", "Aisha", "Buratino", "Chipollino"],
            ["Chernyavskaya_OS@mail.ru", "Aisha_@mail.ru"],
        )
    ]

    session.add_all(users)
    session.commit()

    agents = [
        Agent(name=name)
        for name in zip_longest(
            ["Ремонтная бригада", "Логистическая компания", "Петербургские кухни", "Поставщик материалов", "Магазин"]
        )
    ]

    session.add_all(agents)
    session.commit()

    agents_conn_types = [
        Agent_Conn_Type(description=description)
        for description in zip_longest(
            ["Производство кухни", "Установка кухни", "Замена проводки", "Перевозка мебели", "Перевозка техники", "Капитальный ремонт"]
        )
    ]

    session.add_all(agents_conn_types)
    session.commit()

    tags = [
        Tag(description=description)
        for description in zip_longest(
            ["ремонт", "закупка", "кухня", "переезд"]
        )
    ]

    session.add_all(tags)
    session.commit()

    #получаем id агентов ремонтных услуг:
    repair_agent_id = 0
    repair_agent = get_agent_id_by_name(session, "Ремонтная бригада")
    if repair_agent is not None:
        repair_agent_id = repair_agent.id

    logistic_agent_id = 0
    logistic_agent = get_agent_id_by_name(session, "Логистическая компания")
    if logistic_agent is not None:
        logistic_agent_id = repair_agent.id

    kitchen_agent_id = 0
    kitchen_agent = get_agent_id_by_name(session, "Петербургские кухни")
    if kitchen_agent is not None:
        kitchen_agent_id = repair_agent.id

    #получаем id ремонтных услуг:
    repair_hyzmat_id = 0
    repair_hyzmat = get_conn_type_id_by_desc(session, "Капитальный ремонт")
    if repair_hyzmat is not None:
        repair_hyzmat_id = repair_hyzmat.id

    logistic_hyzmat_id = 0
    logistic_hyzmat = get_conn_type_id_by_desc(session, "Перевозка мебели")
    if logistic_hyzmat is not None:
        logistic_hyzmat_id = logistic_hyzmat.id

    kitchen_hyzmat_id = 0
    kitchen_hyzmat = get_conn_type_id_by_desc(session, "Производство кухни")
    if kitchen_hyzmat is not None:
        kitchen_hyzmat_id = kitchen_hyzmat.id

    agent_conns = [
        Agent_Conn(agent_id=agent_id,
                   conn_type_id=conn_type_id,
                   connection_date=connection_date,
                   done=done,
                   note=note,
                   costs=costs)
        for agent_id, conn_type_id, connection_date, done, note, costs in zip_longest(
            [repair_agent_id, logistic_agent_id, kitchen_agent_id],
            [repair_hyzmat_id, logistic_hyzmat_id, kitchen_hyzmat_id],
            [datetime.today(), datetime.today(), datetime.today()],
            [False, False, True],
            ["завершено на 70%", "завершено на 30%", "завершено на 100%"],
            [600, 50, 120]
        )
    ]
    session.add_all(agent_conns)
    session.commit()

    #получаем id чиполлино и буратино как авторов постов в блоге:
    chipollino_id = 0
    chipollino_user =  get_user_by_username(session, "Chipollino")
    if chipollino_user is not None:
         chipollino_id = chipollino_user.id

    buratino_id = 0
    buratino_user = get_user_by_username(session, "Buratino")
    if buratino_user is not None:
        buratino_id = buratino_user.id

    posts = [
        Post(user_id=user_id, description=description)
        for user_id, description in zip_longest(
            [chipollino_id, buratino_id],
            ["В Петербург я никогда не собирался. Но волею судеб свершился этот переезд. И что же будет дальше?",
             "Далее потянулся ремонт, закупка материалов, и скоро будет еще одна новая кухня."]
        )
    ]
    session.add_all(posts)
    session.commit()

def fetch_all_users(session: Session) -> Sequence[User]:
    stmt = select(User).order_by(User.id)
    return session.scalars(stmt).all()

def get_user_by_username(session: Session, username: str) -> User | None:
    stmt = (
        select(User)
        .where(
            func.lower(User.username)
            == func.lower(username)
        )
    )
    return session.scalars(stmt).first()

def select_user_posts(session: Session, name_user: String, tag: String):
    users = fetch_all_users(session)

    user_target = get_user_by_username(session, name_user)
    user_post = get_post(session, user_target.id)

    if tag in user_post.description:
        print("Пост от {f} с тегом '{t}': ".format(f = name_user, t = tag), user_post.description)

    #len_user_post = 0
    #if tag in user_post.description:
        #len_user_post = len(user_post.description)
        #assert len_user_post > 0  #типа тест, в пакете testing компилятор отдельно не видит структуру классов из declarative_base, поэтому сюда вынесла

def get_post(session: Session, user_id) -> Agent | None:
    stmt = (
        select(Post)
        .where(
            Post.user_id
            == user_id
        )
    )
    return session.scalars(stmt).first()

def get_agent_id_by_name(session: Session, agent_name: str) -> Agent | None:
    stmt = (
        select(Agent)
        .where(
            func.lower(Agent.name)
            == func.lower(agent_name)
        )
    )
    return session.scalars(stmt).first()

def get_conn_type_id_by_desc(session: Session, desc: str) -> Agent_Conn_Type | None:
    stmt = (
        select(Agent_Conn_Type)
        .where(
            func.lower(Agent_Conn_Type.description)
            == func.lower(desc)
        )
    )
    return session.scalars(stmt).first()

def main():
    #create_table()

    with Session(engine) as session:
        #insert_rows(session)
        select_user_posts(session, "Chipollino", "переезд")
        #select_user_posts(session, "Buratino", "кухня")

if __name__ == "__main__":
    main()
