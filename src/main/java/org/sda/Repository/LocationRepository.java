package org.sda.Repository;



import org.sda.Entity.Location;

import java.util.List;
import java.util.UUID;

public interface LocationRepository {

    void addLocation(Location location);
    void updateLocation(Location location);
    void deleteLocation(UUID locationId);
    Location getLocation(UUID locationId);
    Location getLocationByName(String cityName);
    List<Location> getAllLocations();
}
