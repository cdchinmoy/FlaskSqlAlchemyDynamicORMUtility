from sqlalchemy import Column, Integer, String, MetaData, create_engine
engine = create_engine('sqlite:///sales.db', echo = True)
# engine = create_engine("mysql://'rabit':'Chinmoy123!@#'@localhost/dgglib_12feb",echo = True)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
Base = declarative_base()
from logger import LOGGER
from sqlalchemy.exc import IntegrityError, SQLAlchemyError


metadata = MetaData(bind=engine)
Session = sessionmaker(bind = engine)

class Customers(Base):
   __tablename__ = 'customers'
   
   id = Column(Integer, primary_key=True)
   name = Column(String)
   address = Column(String)
   email = Column(String)


Base.metadata.create_all(engine)

def insert(table_name, **values):
    sess = Session()
    table = eval(table_name)
    try:      
        c1 = table(**values)
        sess.add(c1)
        sess.commit()
        LOGGER.success(f"Created new customer: {c1}")
    except IntegrityError as e:
        LOGGER.error(e.orig)
        raise e.orig from e
    except SQLAlchemyError as e:
        LOGGER.error(f"Unexpected error when creating user: {e}")
        raise e


# insert("Customers", name="Chinmoy", address="Kushamndi", email = "cdchinmoy@gmail.com")

def get_select_data(table_name, select):
    sess = Session()

    if select:
        final_str = ''
        for each in select:
            final_str = f"{final_str}{table_name}.{each},"
        final_select = eval(final_str[0:-1])
        data = sess.query(*final_select).all()
    return [dict(zip(c.keys(), c)) for c in data]    

# data = get_select_data("Customers", ["id", "name", "address", "email"])
# print(data)


def get_all_data(table_name, **filter):
    sess = Session()
    if table_name:
        eval_table_name = eval(table_name)
        final_select = ",".join(str(column).capitalize() for column in eval_table_name.__table__.columns)
        final_select = eval(final_select)
        if filter:
            filter_str = ','.join(f'{table_name}.{key} == "{val}"' for key,val in filter.items())
            filter_str = eval(filter_str)
            if len(filter) > 1:
                data = sess.query(*final_select).filter(*filter_str).all()
            else:    
                data = sess.query(*final_select).filter(filter_str).all()    
        else:    
            data = sess.query(*final_select).all()
    return [dict(zip(c.keys(), c)) for c in data]

data = get_all_data("Customers", id="2",name="Chinmoy")   
print(data)