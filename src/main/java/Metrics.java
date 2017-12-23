public class Metrics {
    private static long startTime;
    private static long stopTime;
    private static long usedMemory;

    public static void start() {
        startTime = System.currentTimeMillis();
    }

    public static void stop() {
        stopTime = System.currentTimeMillis();
        usedMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
    }

    public static void getResults() {
        long estimatedTime = stopTime - startTime;
        System.out.printf("Memory used: %d Mb \n", convertToMegabytes(usedMemory));
        System.out.printf("Execution time: %d ms \n", estimatedTime);
    }

    private static long convertToMegabytes(long bytes) {
        return bytes / 1048576;
    }
}
