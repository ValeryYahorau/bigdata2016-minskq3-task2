package com.epam.bigdata2016.minskq3.task2;

public class Utils {

    public static int[] getNumbersSegment(int total, int splits, int current) {
        int startN = 0;
        int endN = total;
        int diff = (int) Math.ceil((total % splits) * splits);

        if (splits != 1) {
            int step = total / splits;
            endN = current * step;
            if (endN + diff < total) {
                startN = endN - step;
            }
            else {
                endN = total;
                startN = endN - step + (current * step - total);
            }
        }
        if (startN == 0 ) {
            startN = 1;
        }
        return new int[]{startN, endN};
    }
}
