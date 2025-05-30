from sqlalchemy import Column, Integer, Text, DateTime, ForeignKey
from sqlalchemy.orm import relationship
from geoalchemy2 import Geometry
from .Base import Base

class RadarFile(Base):
    __tablename__ = 'radar_file'
    id = Column(Integer, primary_key=True)
    radar_id = Column(Integer, ForeignKey('radar.id', ondelete="CASCADE"), nullable=False)
    s3_key = Column(Text, nullable=False)
    processed_at = Column(DateTime, nullable=False)
    file_time = Column(DateTime, nullable=False)
    local_path = Column(Text, nullable=False)
    sweep_fixed_angle = Column(Text, nullable=True)
    radar = relationship("Radar", back_populates="files")
    statistics = relationship("RadarStatistics", back_populates="radar_file", uselist=False)