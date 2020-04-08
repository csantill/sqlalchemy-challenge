import numpy as np
import pandas as pd
import datetime as dt
# Python SQL toolkit and Object Relational Mapper
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
from flask import Flask
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker
from flask import Response

engine = create_engine("sqlite:///Resources/hawaii.sqlite")
# reflect an existing database into a new model
Base = automap_base()

# reflect the tables
Base.prepare(engine, reflect=True)
# Save references to each table
Measurement  = Base.classes.measurement
Station =  Base.classes.station
# Create our session (link) from Python to the DB
session = scoped_session(sessionmaker(engine))


app = Flask(__name__)

@app.route("/")
def home():
    print("In & Out of Home section.")
    return (
        f'Climate API<br/>'
        f'Available Routes:<br/>'
        f'<a href="/api/v1.0/precipitation">/api/v1.0/precipitation </a> <br/> '
        f'<a href="/api/v1.0/stations">/api/v1.0/stations </a> <br/>'
        f'<a href="/api/v1.0/tobs">/api/v1.0/tobs </a> <br/>'
        f'Select a start Date<br/>'
        f'<ul> <li> <a href="/api/v1.0/2016-01-01/">/api/v1.0/2016-01-01/  </a> <br/></li> </ul> '
        f'Select a start Date and end Date<br/>'
        f'<ul> <li><a href="/api/v1.0/2016-01-01/2016-01-31/">/api/v1.0/2016-01-01/2016-01-31/  </a> </li> </ul> <br/>'
    )
@app.route("/api/v1.0/precipitation")
def precipitation():
    """Return the JSON representation of your dictionary."""
    precipitation = (
        session
        .query(Measurement.date, Measurement.prcp)
        .order_by(Measurement.date)
    ).all()
    precipitation_df = pd.DataFrame(precipitation, columns=['Date', 'Precipitation'])
    precipitation_df['Date'] = pd.to_datetime(precipitation_df['Date'], format='%Y/%m/%d')
    precipitation_df = precipitation_df.set_index('Date')

    return Response(precipitation_df.to_json(orient='records'), mimetype='application/json')
@app.route("/api/v1.0/stations")
def stations():
    """Return a JSON list of stations from the dataset."""
    all_stations = (
        session
        .query(Station.station)
        .order_by(Station.station)
    ).all()
    station_df = pd.DataFrame(all_stations, columns=['Station'])        
    return Response(station_df.to_json(orient='records'), mimetype='application/json')

@app.route("/api/v1.0/tobs")
def tobs():

    lastest_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()

    # Calculate the date 1 year ago from the last data point in the database
    first_date = dt.date(2017,8, 23) - dt.timedelta(days=365)
    station_obs = (
    session
    .query(Measurement.station, func.count(Measurement.tobs))
    .group_by(Measurement.station)
    .order_by(func.count(Measurement.tobs)
    .desc()
    )).all()
    stationobsdf = pd.DataFrame(station_obs, columns=['Station', 'Number of Observations'])
    stationobsdf.head()
    top_station = stationobsdf.iloc[0]['Station']
    highestobs = (
    session
    .query(Measurement.date, Measurement.tobs)
    .filter(Measurement.station == top_station)
    .filter(Measurement.date > first_date)
    .order_by(Measurement.date)).all()

    highestobsdf = pd.DataFrame(highestobs, columns=['Station', 'Temperature'])       
    return Response(highestobsdf.to_json(orient='records'), mimetype='application/json')

# Return a JSON list of the minimum temperature, the average temperature, and the max temperature for a given start date
@app.route("/api/v1.0/<start_date>/")
def calc_temps_start(start_date):
    select = ([
         func.min(Measurement.tobs), 
         func.avg(Measurement.tobs), 
         func.max(Measurement.tobs)
         ])
    cols = ['min', 'avg', 'max' ]
    result_temp = (
        session
        .query(*select)
        .filter(Measurement.date >= start_date)
        ).all()
    result_df = pd.DataFrame(result_temp, columns=cols)       
    return Response(result_df.to_json(orient='records') , mimetype='application/json')

    
@app.route("/api/v1.0/<start_date>/<end_date>/")
def calc_temps_start_end(start_date, end_date):
    select = ([
         func.min(Measurement.tobs), 
         func.avg(Measurement.tobs), 
         func.max(Measurement.tobs)
         ])
    cols =  ['min', 'avg', 'max' ]          
    result_temp = (
        session
        .query(*select)
        .filter(Measurement.date >= start_date)
        .filter(Measurement.date <= end_date)
    ).all()
    result_df = pd.DataFrame(result_temp, columns=cols)   
    return Response(result_df.to_json(orient='records') , mimetype='application/json')



if __name__ == '__main__':
    app.run(debug=True)