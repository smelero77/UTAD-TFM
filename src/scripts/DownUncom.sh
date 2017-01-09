#!/usr/bin/env bash
rm /home/sergio/TFM_Ficheros/Flights/*
rm /home/sergio/TFM_Ficheros/Airports/*
rm /home/sergio/TFM_Ficheros/Carriers/*
rm /home/sergio/TFM_Ficheros/PlaneData/*


wget http://stat-computing.org/dataexpo/2009/{1987..2008}.csv.bz2  -P /home/sergio/TFM_Ficheros/Flights
wget http://stat-computing.org/dataexpo/2009/airports.csv -P /home/sergio/TFM_Ficheros/Airports
wget http://stat-computing.org/dataexpo/2009/carriers.csv -P /home/sergio/TFM_Ficheros/Carriers
wget http://stat-computing.org/dataexpo/2009/plane-data.csv -P /home/sergio/TFM_Ficheros/PlaneData


cd /home/sergio/TFM_Ficheros/Flights

bunzip2 *.bz2
