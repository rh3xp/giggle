from sqlalchemy import Column, Integer, DateTime
from datetime import datetime
# from .database import Base
from database import Base

class Timestamp(Base):
    __tablename__ = "timestamps"
    id = Column(Integer, primary_key=True, index=True)
    accessed_at = Column(DateTime, default=datetime.utcnow)
