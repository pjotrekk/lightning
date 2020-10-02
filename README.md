# Lightning app
The purpose is to demonstrate how to use Beam in a simple Java Spring application.

# How to run it
Download the sources and run the following commands from the app root directory:
```
docker-compose up
```
This will set up the Kafka and MongoDB instances
```
./mvnw spring-boot:run
```
This will build and run the application.

You can also open the project in IntelliJ and run it via `LightningApplication` main method.

# How to use it
The app exposes two endpoints: 
```
http://localhost:8080/lightning
``` 
with one possible parameter - timestamp in milliseconds. It gives us all lightning json data with timestamp later than
the one from the param. Example:
```
http://localhost:8080/lightning?timestamp=1600760252089
```
The lightning data is in json format:
```
{
  id: <string>,
  power: <int>,
  strokeTheGround: <bool>,
  timestamp: <int>,
  coordinates: {
    longitude: <real>,
    latitude: <real>
  }
}
```
and
```
http://localhost:8080/strikes
```
which returns count of the lightnings that stroke the ground in the interval of 1 minute.

```
{
  strikes: <int>,
  from: <timestamp_ms>,
  to: <timestamp_ms>
}
```