package org.example;

/**
 * @author rillusory
 * @Description
 * @date 11/18/24 8:22 PM
 **/
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class SpeedCalculator {

    // Calculate the distance between two coordinates using Haversine formula
    public static double calculateDistance(double lat1, double lon1, double lat2, double lon2) {
        final int EARTH_RADIUS = 6371; // Radius of the earth in km
        double latDistance = Math.toRadians(lat2 - lat1);
        double lonDistance = Math.toRadians(lon2 - lon1);
        double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return EARTH_RADIUS * c; // Distance in kilometers
    }

    // Calculate the speed between two events
    public static double calculateSpeed(
            String timestamp1, String timestamp2,
            double lat1, double lon1, double lat2, double lon2) {
        // Parse timestamps
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime time1 = LocalDateTime.parse(timestamp1, formatter);
        LocalDateTime time2 = LocalDateTime.parse(timestamp2, formatter);

        // Calculate time difference in hours
        double timeDifference = Math.abs(
                time1.toInstant(ZoneOffset.UTC).toEpochMilli()
                        - time2.toInstant(ZoneOffset.UTC).toEpochMilli()) / (1000.0 * 3600.0);

        // Calculate distance in kilometers
        double distance = calculateDistance(lat1, lon1, lat2, lon2);

        // Avoid division by zero
        if (timeDifference == 0) {
            return Double.MAX_VALUE;
        }

        // Calculate speed (km/h)
        return distance / timeDifference;
    }

    // Test the function
    public static void main(String[] args) {
        String timestamp1 = "2024-11-18T08:00:00.000";
        String timestamp2 = "2024-11-18T10:00:00.000";
        double lat1 = 41.8781; // Chicago
        double lon1 = -87.6298;
        double lat2 = 40.7128; // New York
        double lon2 = -74.0060;

        double speed = calculateSpeed(timestamp1, timestamp2, lat1, lon1, lat2, lon2);
        System.out.println("Calculated Speed: " + speed + " km/h");
    }
}