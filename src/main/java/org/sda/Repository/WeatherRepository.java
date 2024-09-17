package org.sda.Repository;



import org.sda.Entity.WeatherData;

import java.time.LocalDate;
import java.util.List;
import java.util.UUID;

public interface WeatherRepository {

        void addWeatherData(WeatherData weatherData);

        List<WeatherData> getWeatherDataByLocation(UUID locationId);

        List<WeatherData> getWeatherDataByLocationAndDateRange(UUID locationId, LocalDate startDate, LocalDate endDate);

}
