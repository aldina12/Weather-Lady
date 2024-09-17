package org.sda.Connection;

import java.sql.*;

public class DatabaseCreator {

    public DatabaseCreator() {
        createDatabaseIfNotExists();
        createLocationsTableIfNotExists();
        createWeatherDataTableIfNotExists();
    }

    private void createDatabaseIfNotExists() {
        try (Connection connection = DriverManager.getConnection(Constants.DB_URL, Constants.DB_USERNAME, Constants.DB_PASSWORD)) {
            Statement statement = connection.createStatement();
            String query = String.format("CREATE DATABASE IF NOT EXISTS %s", Constants.DB_NAME);
            statement.executeUpdate(query);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void createLocationsTableIfNotExists() {
        try (Connection connection = DriverManager.getConnection(Constants.DB_URL + Constants.DB_NAME, Constants.DB_USERNAME, Constants.DB_PASSWORD)) {
            String query = "CREATE TABLE IF NOT EXISTS POLICI (" +
                    "ID VARCHAR(36) PRIMARY KEY, " +
                    "CITY_NAME VARCHAR(255), " +
                    "REGION VARCHAR(255), " +
                    "COUNTRY_NAME VARCHAR(255))";
            PreparedStatement statement = connection.prepareStatement(query);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void createWeatherDataTableIfNotExists() {
        try (Connection connection = DriverManager.getConnection(Constants.DB_URL + Constants.DB_NAME, Constants.DB_USERNAME, Constants.DB_PASSWORD)) {
            String query = "CREATE TABLE IF NOT EXISTS MODERATOR (" +
                    "ID VARCHAR(36) PRIMARY KEY, " +
                    "LOCATION_ID VARCHAR(36), " +
                    "DATE DATE, " +
                    "TEMPERATURE DOUBLE, " +
                    "FEELS_LIKE DOUBLE, " +
                    "PRESSURE DOUBLE, " +
                    "HUMIDITY DOUBLE, " +
                    "WIND_DIRECTION VARCHAR(255), " +
                    "WIND_SPEED DOUBLE, " +
                    "WEATHER_DESCRIPTION VARCHAR(255), " +
                    "LONGITUDE DOUBLE, " +
                    "LATITUDE DOUBLE, " +
                    "SUNRISE DATE, " +
                    "SUNSET DATE, " +
                    "VISIBILITY DOUBLE)";
            PreparedStatement statement = connection.prepareStatement(query);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}