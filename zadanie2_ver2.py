import csv

from typing import List
from typing import Optional
from sqlalchemy import ForeignKey
from sqlalchemy import String
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship
from sqlalchemy import create_engine
from sqlalchemy.orm import Session


class Base(DeclarativeBase):
    pass

class Stations(Base):
    __tablename__ = "stations"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    station_id: Mapped[str]
    latitude: Mapped[float] 
    longitude: Mapped[float]
    elevation: Mapped[float]
    name: Mapped[str]
    country: Mapped[str]
    state: Mapped[str]
    
         


class Measures(Base):
    __tablename__ = "measures"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    station_id: Mapped[Stations] = mapped_column(ForeignKey("stations"))
    date: Mapped[str]
    precip: Mapped[float]
    tobs: Mapped[float]


    
with open('clean_stations.csv', newline='') as csvfile:

    reader= csv.reader(csvfile, delimiter=',', quotechar='|')
    stations = [station for station in reader]
    
with open('clean_measure.csv', newline='') as csvfile:

    reader= csv.reader(csvfile, delimiter=' ', quotechar='|')
    measures = [measure for measure in reader]

measures = [measures[i][0].split(",") for i in range(len(measures))] 

engine = create_engine('sqlite:///database3.db')

Base.metadata.create_all(engine)

with Session(engine) as session:
    for station in stations[1:]:
        session.add(Stations(
            station_id = station[0],
            latitude = float(station[1]),
            longitude = float(station[2]),
            elevation = float(station[3]),
            name = station[4],
            country = station[5], 
            state = station[6], 
        ))
        for measure in measures[1:]:
            session.add(Measures(   
            station_id = station[0],                               
            date = measure[1],
            precip = float(measure[2]),
            tobs = float(measure[3])          
            ))                    
            
    
    session.commit()