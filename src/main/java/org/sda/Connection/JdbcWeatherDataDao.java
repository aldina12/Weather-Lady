package org.sda.Connection;


import org.sda.Entity.WeatherData;
import org.sda.Repository.WeatherRepository;

import java.sql.*;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class JdbcWeatherDataDao implements WeatherRepository {



    @Override
    public void addWeatherData(WeatherData weatherData) {
        try {
            Connection connection = DriverManager.getConnection(Constants.DB_URL + Constants.DB_NAME, Constants.DB_USERNAME, Constants.DB_PASSWORD);
            String query = "INSERT INTO WEATHER_DATA (ID, LOCATION_ID, DATE, TEMPERATURE, FEELS_LIKE, PRESSURE, HUMIDITY, WIND_DIRECTION, WIND_SPEED, WEATHER_DESCRIPTION, LONGITUDE, LATITUDE, SUNRISE, SUNSET, VISIBILITY) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            PreparedStatement statement = connection.prepareStatement(query);
            statement.setString(1, weatherData.getId().toString());
            statement.setString(2, weatherData.getLocationId().toString());
            statement.setDate(3, Date.valueOf(weatherData.getDate()));
            statement.setDouble(4, weatherData.getTemperature());
            statement.setDouble(5, weatherData.getFeelsLike());
            statement.setDouble(6, weatherData.getPressure());
            statement.setDouble(7, weatherData.getHumidity());
            statement.setString(8, weatherData.getWindDirection());
            statement.setDouble(9, weatherData.getWindSpeed());
            statement.setString(10, weatherData.getWeatherDescription());
            statement.setDouble(11, weatherData.getLongitude());
            statement.setDouble(12, weatherData.getLatitude());
            statement.setDate(13, Date.valueOf(weatherData.getSunrise()));
            statement.setDate(14, Date.valueOf(weatherData.getSunset()));
            statement.setDouble(15, weatherData.getVisibility());

            statement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public List<WeatherData> getWeatherDataByLocation(UUID locationId) {
        List<WeatherData> weatherDataList = new ArrayList<>();
        try {
            Connection connection = DriverManager.getConnection(Constants.DB_URL + Constants.DB_NAME, Constants.DB_USERNAME, Constants.DB_PASSWORD);
            String query = "SELECT * FROM WEATHER_DATA WHERE LOCATION_ID = ?";
            PreparedStatement statement = connection.prepareStatement(query);
            statement.setString(1, locationId.toString());
            ResultSet resultSet = statement.executeQuery();

            while (resultSet.next()) {
                WeatherData weatherData = new WeatherData();
                weatherData.setId(UUID.fromString(resultSet.getString("ID")));
                weatherData.setLocationId(UUID.fromString(resultSet.getString("LOCATION_ID")));
                weatherData.setDate(resultSet.getDate("DATE").toLocalDate());
                weatherData.setTemperature(resultSet.getDouble("TEMPERATURE"));
                weatherData.setFeelsLike(resultSet.getDouble("FEELS_LIKE"));
                weatherData.setPressure(resultSet.getDouble("PRESSURE"));
                weatherData.setHumidity(resultSet.getDouble("HUMIDITY"));
                weatherData.setWindDirection(resultSet.getString("WIND_DIRECTION"));
                weatherData.setWindSpeed(resultSet.getDouble("WIND_SPEED"));
                weatherData.setWeatherDescription(resultSet.getString("WEATHER_DESCRIPTION"));
                weatherData.setLongitude(resultSet.getDouble("LONGITUDE"));
                weatherData.setLatitude(resultSet.getDouble("LATITUDE"));
                weatherData.setSunrise(resultSet.getDate("SUNRISE").toLocalDate());
                weatherData.setSunset(resultSet.getDate("SUNSET").toLocalDate());
                weatherData.setVisibility(resultSet.getDouble("VISIBILITY"));

                weatherDataList.add(weatherData);
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return weatherDataList;
    }


    @Override
    public List<WeatherData> getWeatherDataByLocationAndDateRange(UUID locationId, LocalDate startDate, LocalDate endDate) {
        List<WeatherData> weatherDataList = new ArrayList<>();
        try (Connection connection = DriverManager.getConnection(Constants.DB_URL + Constants.DB_NAME, Constants.DB_USERNAME, Constants.DB_PASSWORD)) {

            String query = "SELECT * FROM WEATHER_DATA WHERE LOCATION_ID = ? AND DATE >= ? AND DATE <= ?";
            PreparedStatement statement = connection.prepareStatement(query);
            statement.setString(1, locationId.toString());
            statement.setDate(2, Date.valueOf(startDate));
            statement.setDate(3, Date.valueOf(endDate));
            ResultSet resultSet = statement.executeQuery();

            while (resultSet.next()) {
                WeatherData weatherData = new WeatherData();
                weatherData.setId(UUID.fromString(resultSet.getString("ID")));
                weatherData.setLocationId(UUID.fromString(resultSet.getString("LOCATION_ID")));
                weatherData.setDate(resultSet.getDate("DATE").toLocalDate());
                weatherData.setTemperature(resultSet.getDouble("TEMPERATURE"));
                weatherData.setFeelsLike(resultSet.getDouble("FEELS_LIKE"));
                weatherData.setPressure(resultSet.getDouble("PRESSURE"));
                weatherData.setHumidity(resultSet.getDouble("HUMIDITY"));
                weatherData.setWindDirection(resultSet.getString("WIND_DIRECTION"));
                weatherData.setWindSpeed(resultSet.getDouble("WIND_SPEED"));
                weatherData.setWeatherDescription(resultSet.getString("WEATHER_DESCRIPTION"));
                weatherData.setLongitude(resultSet.getDouble("LONGITUDE"));
                weatherData.setLatitude(resultSet.getDouble("LATITUDE"));
                weatherData.setSunrise(resultSet.getDate("SUNRISE").toLocalDate());
                weatherData.setSunset(resultSet.getDate("SUNSET").toLocalDate());
                weatherData.setVisibility(resultSet.getDouble("VISIBILITY"));

                weatherDataList.add(weatherData);
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return weatherDataList;
    }


}
