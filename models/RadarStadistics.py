from sqlalchemy import Column, Integer, Float, DateTime, ForeignKey
from geoalchemy2 import Geometry
from .Base import Base
from sqlalchemy.orm import relationship

class RadarStatistics(Base):
    __tablename__ = 'radar_statistics'
    id = Column(Integer, primary_key=True)
    radar_file_id = Column(Integer, ForeignKey('radar_file.id', ondelete="CASCADE"), nullable=False, unique=True)
    mean_reflectivity = Column(Float)
    max_reflectivity = Column(Float)
    min_reflectivity = Column(Float)
    rain_area_percent = Column(Float)
    duration_minutes = Column(Integer)
    event_bbox = Column(Geometry('POLYGON', srid=4326))
    created_at = Column(DateTime)
    radar_file = relationship("RadarFile", back_populates="statistics")