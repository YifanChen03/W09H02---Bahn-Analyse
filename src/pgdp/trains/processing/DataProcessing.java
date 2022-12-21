package pgdp.trains.processing;

import pgdp.trains.connections.Station;
import pgdp.trains.connections.TrainConnection;
import pgdp.trains.connections.TrainStop;

import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DataProcessing {

    public static Stream<TrainConnection> cleanDataset(Stream<TrainConnection> connections) {
        // TODO Task 1.
        return connections
                .distinct()
                .sorted(Comparator.comparing(tc -> tc.getFirstStop().scheduled()))
                .map(tc -> tc.withUpdatedStops(
                        tc.stops().stream()
                                .filter(ts -> !ts.kind().equals(TrainStop.Kind.CANCELLED))
                                .collect(Collectors.toList())
                ));
    }

    public static TrainConnection worstDelayedTrain(Stream<TrainConnection> connections) {
        // TODO Task 2.
        List<TrainConnection> saveConnections = connections
                .collect(Collectors.toList());
        try {
            List<Integer> sortedConnectionsForDelay =
                    saveConnections.stream()
                            .map(tc -> tc.stops().stream()
                                    .mapToInt(ts -> ts.getDelay())
                                    .max())
                            .map(dmax -> dmax.getAsInt())
                            .collect(Collectors.toList());

            if (sortedConnectionsForDelay.get(sortedConnectionsForDelay.size() - 1) == 0) {
                return saveConnections.get(0);
            }

            List<TrainConnection> wd_tc = saveConnections.stream()
                    .filter(tc -> (tc.stops().stream()
                            .mapToInt(ts -> ts.getDelay())
                            .max().getAsInt()) == sortedConnectionsForDelay.get(sortedConnectionsForDelay.size() - 1))
                    .collect(Collectors.toList());
            //System.out.println(wd_tc.get(0).stops().stream().mapToInt(tsList -> tsList.getDelay()).max());
            //System.out.println(sortedConnectionsForDelay);
            return wd_tc.get(0);
        } catch (Exception e) {
            return null;
        }
    }

    public static double percentOfKindStops(Stream<TrainConnection> connections, TrainStop.Kind kind) {
        // TODO Task 3.
        List<TrainConnection> saveConnections = connections
                .collect(Collectors.toList());
        //berechne wie oft ein stop mit kind vorkommt
        double output = saveConnections.stream()
                .mapToInt(tc -> tc.stops().stream()
                                .mapToInt(ts -> ts.kind().equals(kind) ? 1 : 0)
                                        .sum())
                        .sum();
        //teile durch gesamtanzahl von einträgen
        output = output / saveConnections.stream()
                .mapToInt(tc -> tc.stops().size())
                .sum();
        return output * 100;
    }

    public static double averageDelayAt(Stream<TrainConnection> connections, Station station) {
        // TODO Task 4.
        return 0.0;
    }

    public static Map<String, Double> delayComparedToTotalTravelTimeByTransport(Stream<TrainConnection> connections) {
        // TODO Task 5.
        return null;
    }

    public static Map<Integer, Double> averageDelayByHour(Stream<TrainConnection> connections) {
        // TODO Task 6.
        return null;
    }

    public static void main(String[] args) {
        // Um alle Verbindungen aus einer Datei zu lesen, verwendet DataAccess.loadFile("path/to/file.json"), etwa:
        // Stream<TrainConnection> trainConnections = DataAccess.loadFile("connections_test/fullDay.json");

        // Oder alternativ über die API, dies aber bitte sparsam verwenden, um die API nicht zu überlasten.
        //Stream<TrainConnection> trainsMunich = DataAccess.getDepartureBoardNowFor(Station.MUENCHEN_HBF);

        List<TrainConnection> trainConnections = List.of(
                new TrainConnection("ICE 2", "ICE", "2", "DB", List.of(
                        new TrainStop(Station.MUENCHEN_HBF,
                                LocalDateTime.of(2022, 12, 1, 11, 0),
                                LocalDateTime.of(2022, 12, 1, 11, 0),
                                TrainStop.Kind.REGULAR),
                        new TrainStop(Station.NUERNBERG_HBF,
                                LocalDateTime.of(2022, 12, 1, 11, 30),
                                LocalDateTime.of(2022, 12, 1, 12, 0),
                                TrainStop.Kind.REGULAR)
                )),
                new TrainConnection("ICE 1", "ICE", "1", "DB", List.of(
                        new TrainStop(Station.MUENCHEN_HBF,
                                LocalDateTime.of(2022, 12, 1, 10, 0),
                                LocalDateTime.of(2022, 12, 1, 10, 0),
                                TrainStop.Kind.REGULAR),
                        new TrainStop(Station.NUERNBERG_HBF,
                                LocalDateTime.of(2022, 12, 1, 10, 30),
                                LocalDateTime.of(2022, 12, 1, 10, 30),
                                TrainStop.Kind.REGULAR)
                )),
                new TrainConnection("ICE 3", "ICE", "3", "DB", List.of(
                        new TrainStop(Station.MUENCHEN_HBF,
                                LocalDateTime.of(2022, 12, 1, 12, 0),
                                LocalDateTime.of(2022, 12, 1, 12, 0),
                                TrainStop.Kind.REGULAR),
                        new TrainStop(Station.AUGSBURG_HBF,
                                LocalDateTime.of(2022, 12, 1, 12, 20),
                                LocalDateTime.of(2022, 12, 1, 13, 0),
                                TrainStop.Kind.CANCELLED),
                        new TrainStop(Station.NUERNBERG_HBF,
                                LocalDateTime.of(2022, 12, 1, 13, 30),
                                LocalDateTime.of(2022, 12, 1, 13, 30),
                                TrainStop.Kind.REGULAR)
                ))
        );

        List<TrainConnection> cleanDataset = cleanDataset(trainConnections.stream()).toList();
        // cleanDataset sollte sortiert sein: [ICE 1, ICE 2, ICE 3] und bei ICE 3 sollte der Stopp in AUGSBURG_HBF
        // nicht mehr enthalten sein.

        TrainConnection worstDelayedTrain = worstDelayedTrain(trainConnections.stream());
        //System.out.println(worstDelayedTrain);
        // worstDelayedTrain sollte ICE 3 sein. (Da der Stop in AUGSBURG_HBF mit 40 Minuten Verspätung am spätesten ist.)

        double percentOfKindStops = percentOfKindStops(trainConnections.stream(), TrainStop.Kind.REGULAR);
        System.out.println(percentOfKindStops);
        // percentOfKindStops REGULAR sollte 85.71428571428571 sein, CANCELLED 14.285714285714285.

        double averageDelayAt = averageDelayAt(trainConnections.stream(), Station.NUERNBERG_HBF);
        // averageDelayAt sollte 10.0 sein. (Da dreimal angefahren und einmal 30 Minuten Verspätung).

        Map<String, Double> delayCompared = delayComparedToTotalTravelTimeByTransport(trainConnections.stream());
        // delayCompared sollte ein Map sein, die für ICE den Wert 16.666666666666668 hat.
        // Da ICE 2 0:30 geplant hatte, aber 1:00 gebraucht hat, ICE 1 0:30 geplant und gebraucht hatte, und
        // ICE 3 1:30 geplant und gebraucht hat. Zusammen also 2:30 geplant und 3:00 gebraucht, und damit
        // (3:00 - 2:30) / 3:00 = 16.666666666666668.

        Map<Integer, Double> averageDelayByHourOfDay = averageDelayByHour(trainConnections.stream());
        // averageDelayByHourOfDay sollte ein Map sein, die für 10, 11 den Wert 0.0 hat, für 12 den Wert 15.0 und
        // für 13 den Wert 20.0.

    }


}
