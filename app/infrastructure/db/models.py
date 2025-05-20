from sqlalchemy import Column, Integer, String
from app.core.database import Base

class ETLResult(Base):
    __tablename__ = "etl_results"
    id = Column(Integer, primary_key=True, index=True)
    source = Column(String, index=True)
    result = Column(String)

