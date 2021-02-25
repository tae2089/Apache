package airline.domain;

import org.apache.hadoop.io.Text;

public class AirLineInfo {

    private  int year=0;
    private int month=0;
    private int arriverDelayTime=0;
    private int departureDelayTime=0;
    private int distance=0;

    private boolean arriveDelayAvailable = true;
    private boolean departureDelayAvailable = true;
    private boolean distanceAvailable = true;

    private String uniqueCarrier;

    public AirLineInfo(Text text) {
        String [] colums = text.toString().split(",");
        this.year = Integer.parseInt(colums[0]);
        this.month = Integer.parseInt(colums[1]);

        if (!colums[15].equals("NA")) {
            this.departureDelayTime = Integer.parseInt(colums[15]);
        }else{
            this.departureDelayAvailable= false;
        }

        if (!colums[14].equals("NA")) {
            this.arriverDelayTime = Integer.parseInt(colums[14]);
        }else{
            this.arriveDelayAvailable = false;
        }

        if (!colums[18].equals("NA")) {
            this.distance = Integer.parseInt(colums[18]);
        }else{
            this.distanceAvailable = false;
        }

        this.uniqueCarrier = colums[8];
    }


    public int getYear() {
        return year;
    }

    public int getMonth() {
        return month;
    }

    public int getArriverDelayTime() {
        return arriverDelayTime;
    }

    public int getDepartureDelayTime() {
        return departureDelayTime;
    }

    public int getDistance() {
        return distance;
    }

    public boolean isArriveDelayAvailable() {
        return arriveDelayAvailable;
    }

    public boolean isDepartureDelayAvailable() {
        return departureDelayAvailable;
    }

    public boolean isDistanceAvailable() {
        return distanceAvailable;
    }

    public String getUniqueCarrier() {
        return uniqueCarrier;
    }
}
