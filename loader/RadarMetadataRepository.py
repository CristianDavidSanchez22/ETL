from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
from geoalchemy2.shape import from_shape
from shapely.geometry import Point

from models.Radar import Radar
from models.RadarFile import RadarFile
from models.Base import Base
from models.RadarStadistics import RadarStatistics

class RadarMetadataRepository:
    def __init__(self, db_url: str):
        self.engine = create_engine(db_url)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()

    def close(self):
        self.session.close()
    
    def _parse_point(self, value):
        """Convierte un string 'SRID=4326;POINT(lon lat)' a un objeto geométrico."""
        if isinstance(value, str) and value.startswith("SRID=4326;POINT"):
            coords = value.split("(")[1].strip(")").split()
            point = Point(float(coords[0]), float(coords[1]))
            return from_shape(point, srid=4326)
        return value

    def get_radar_id(self, radar_name: str, radar_location) -> int:
        radar = self.session.query(Radar).filter_by(name=radar_name).first()
        if radar:
            return radar.id
        radar = Radar(name=radar_name, radar_location=radar_location)
        self.session.add(radar)
        self.session.commit()
        return radar.id

    def get_processed_files(self, radar_name: str) -> set[str]:
        radar = self.session.query(Radar).filter_by(name=radar_name).first()
        if not radar:
            return set()
        files = self.session.query(RadarFile.s3_key).filter_by(radar_id=radar.id).all()
        return {f[0] for f in files}

    def insert_metadata(self, record: dict):
        radar_location = self._parse_point(record["bbox"])
        radar_id = self.get_radar_id(record["radar_name"],radar_location)
        radar_file = RadarFile(
            radar_id=radar_id,
            s3_key=record["s3_key"],
            processed_at=record["processed_at"],
            file_time=record["processed_at"],
            local_path=record["local_path"]
        )
        self.session.add(radar_file)
        try:
            self.session.commit()
        except IntegrityError:
            self.session.rollback()