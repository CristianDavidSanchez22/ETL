from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
from geoalchemy2.shape import from_shape
from shapely.geometry import Point, shape

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
            file_time= record["file_time"],
            local_path=record["local_path"],
            sweep_fixed_angle=record["sweep_fixed_angle"],
        )
        self.session.add(radar_file)
        try:
            self.session.commit()
            return radar_file.id
        except IntegrityError:
            self.session.rollback()
            return None
    
    def insert_statistics(self, radar_file_id: int, stats: dict):
        """
        Inserta una fila en RadarStatistics.
        stats debe ser un dict con las claves:
        mean_reflectivity, max_reflectivity, min_reflectivity, rain_area_percent,
        duration_minutes, event_bbox (GeoJSON o shapely), created_at (opcional)
        """
        event_bbox = stats.get("event_bbox")
        if event_bbox is not None and not hasattr(event_bbox, 'geom_type'):
            event_bbox = from_shape(shape(event_bbox), srid=4326)
        elif event_bbox is not None:
            event_bbox = from_shape(event_bbox, srid=4326)

        radar_stats = RadarStatistics(
            file_id=radar_file_id,
            mean_reflectivity=stats.get("mean_reflectivity"),
            max_reflectivity=stats.get("max_reflectivity"),
            min_reflectivity=stats.get("min_reflectivity"),
            rain_area_percent=stats.get("rain_area_percent"),
            duration_minutes=stats.get("duration_minutes"),
            bbox=event_bbox,
            created_at=stats.get("created_at")
        )
        self.session.add(radar_stats)
        try:
            self.session.commit()
        except IntegrityError:
            self.session.rollback()