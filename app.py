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
        sess.add(c1) #Add
        sess.commit() #Commit
        LOGGER.success(f"Created new record: {c1}")
    except IntegrityError as e:
        LOGGER.error(e.orig)
        raise e.orig from e
    except SQLAlchemyError as e:
        LOGGER.error(f"Unexpected error when adding record: {e}")
        raise e


# insert("Customers", name="Mrinmoy", address="Kolkata", email = "cdmrinmoy@gmail.com")

def get_select_data(table_name, *select, **filter):
    sess = Session()
    if select:
        final_select = ",".join(f'{table_name}.{each}' for each in select)
        final_select = eval(final_select)
        if len(select) > 1:
            if filter:
                filter_str = ','.join(f'{table_name}.{key} == "{val}"' for key,val in filter.items())
                filter_str = eval(filter_str)
                if len(filter) > 1:
                    data = sess.query(*final_select).filter(*filter_str).all()
                else:
                    data = sess.query(*final_select).filter(filter_str).all()
            else:    
                data = sess.query(*final_select).all()

        else:
            if filter:
                filter_str = ','.join(f'{table_name}.{key} == "{val}"' for key,val in filter.items())
                filter_str = eval(filter_str)
                if len(filter) > 1:
                    data = sess.query(final_select).filter(*filter_str).all()
                else:
                    data = sess.query(final_select).filter(filter_str).all()
            else:
                data = sess.query(final_select).all()

    LOGGER.info(
        f"Selected {len(data)} row: \
        {data}"
    )
    return [dict(zip(c.keys(), c)) for c in data]    

'''First Paramiter has to be table name and rest should be field name comma separeted'''
# data = get_select_data("Customers", "id", "name", "address", "email", name="Chinmoy")



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
    
    LOGGER.info(
        f"Selected {len(data)} row: \
        {data}"
    )
    return [dict(zip(c.keys(), c)) for c in data]

# data = get_all_data("Customers", name="Chinmoy")   


def delete_data(table_name, **filter):
    sess = Session()
    eval_table_name = eval(table_name)
    try:
        if filter:
            filter_str = ','.join(f'{key} == "{val}"' for key,val in filter.items())
            filter_str = eval(filter_str)
            if len(filter) > 1:
                sess.query(eval_table_name).filter(*filter_str).detete()
                sess.commit()
                LOGGER.success(f"Deleted record: {filter} from table: {table_name}")
            else:

                query = sess.query(eval_table_name).filter(filter_str).get()
                sess.delete(query)
                sess.commit()
                LOGGER.success(f"Deleted record: {filter} from table: {table_name}")
        else:
            sess.delete(eval_table_name)  # Delete
            sess.commit()  # Commit
            LOGGER.success(f"Deleted: {table_name}")
    except IntegrityError as e:
        LOGGER.error(e.orig)
        raise e.orig
    except SQLAlchemyError as e:
        LOGGER.error(f"Unexpected error when deleting table: {e}")
        raise e


delete_data("Customers", id=1)        