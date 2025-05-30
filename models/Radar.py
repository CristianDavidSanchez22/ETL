from geoalchemy2 import Geometry
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import relationship
from .Base import Base

class Radar(Base):
    __tablename__ = 'radar'
    id = Column(Integer, primary_key=True)
    name = Column(String(64), unique=True, nullable=False)
    radar_location = Column(Geometry('POINT', srid=4326))
    files = relationship("RadarFile", back_populates="radar")