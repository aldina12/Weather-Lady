package org.sda.Entity;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDate;
import java.util.UUID;

@AllArgsConstructor
@NoArgsConstructor
@Setter
@Getter
public class WeatherData {

    private UUID id;
    private UUID locationId;
    private LocalDate date;
    private double temperature;
    private double feelsLike;
    private double pressure;
    private double humidity;
    private String windDirection;
    private double windSpeed;
    private String weatherDescription;
    private double longitude;
    private double latitude;
    private LocalDate sunrise;
    private LocalDate sunset;
    private  double visibility;

}
