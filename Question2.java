package com.mycompany.question2;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class Question2 {

    static class grammesKeimenou {
        String episodio;
        String topothesia;
        String xaraktiras;
        String atakes;

        grammesKeimenou(String episodio, String topothesia, String xaraktiras, String atakes) {
            this.episodio = episodio;
            this.topothesia = topothesia;
            this.xaraktiras = xaraktiras;
            this.atakes = atakes;
        }
    }

    static class Worker implements Callable<Map<String, Object>> {
        List<grammesKeimenou> lines;

        Worker(List<grammesKeimenou> lines) {
            this.lines = lines;
        }

        @Override
        public Map<String, Object> call() {
            Map<String, Integer> lekseisEpisodiou = new HashMap<>();
            Map<String, Integer> dialogoiTopothesias = new HashMap<>();
            Map<String, Map<String, Integer>> lekseisXaraktira = new HashMap<>();

            String[] protagonistes = {"Homer Simpson", "Bart Simpson", "Marge Simpson", "Lisa Simpson"};

            for (grammesKeimenou line : lines) {
                // Αριθμος λεξεων επεισοδιου
                int arithmosGramon = line.atakes.split("\\s+").length;
                lekseisEpisodiou.merge(line.episodio, arithmosGramon, Integer::sum);

                // Αριθμος γεγονοτων περιοχων
                dialogoiTopothesias.merge(line.topothesia, 1, Integer::sum);

                // Λεξεις για Bart/Homer/Marge/Lisa
                for (String c : protagonistes) {
                    if (line.xaraktiras != null && line.xaraktiras.equalsIgnoreCase(c)) {
                        Map<String, Integer> wordFreq = lekseisXaraktira.computeIfAbsent(c, k -> new HashMap<>());
                        for (String w : line.atakes.toLowerCase().split("[^a-zA-Z]+")) {
                            if (w.length() >= 5)
                                wordFreq.merge(w, 1, Integer::sum);
                        }
                    }
                }
            }

            // Επιστρεφουμε ολα τα μερικα αποτελεσματα
            Map<String, Object> result = new HashMap<>();
            result.put("lekseisEpisodiou", lekseisEpisodiou);
            result.put("dialogoiTopothesias", dialogoiTopothesias);
            result.put("lekseisXaraktira", lekseisXaraktira);
            return result;
        }
    }

    public static void main(String[] args) throws Exception {
        String fakelos = "C:\\Users\\Xristoforos\\Documents\\NetBeansProjects\\Question2\\src\\main\\java\\com\\mycompany\\question2\\simpsons_script_lines.csv";
        int k = 8; // αριθμος νηματων

        List<grammesKeimenou> allLines = loadData(fakelos);
        System.out.println("Loaded " + allLines.size() + " lines.");

        // Χωρισμος σε k νηματα
        int komatia = allLines.size() / k;
        ExecutorService executor = Executors.newFixedThreadPool(k);
        List<Future<Map<String, Object>>> futures = new ArrayList<>();
        
        long startTime = System.currentTimeMillis();
        
        for (int i = 0; i < k; i++) {
            int start = i * komatia;
            int end = (i == k - 1) ? allLines.size() : start + komatia;
            futures.add(executor.submit(new Worker(allLines.subList(start, end))));
        }

        // Προσθεση αποτελεσματων
        Map<String, Integer> sinolikesLekseis = new HashMap<>();
        Map<String, Integer> dialogoiTopothesias = new HashMap<>();
        Map<String, Map<String, Integer>> sinoloLekseonCharaktiron = new HashMap<>();

        for (Future<Map<String, Object>> f : futures) {
            Map<String, Object> part = f.get();

            mergeCounts(sinolikesLekseis, (Map<String, Integer>) part.get("lekseisEpisodiou"));
            mergeCounts(dialogoiTopothesias, (Map<String, Integer>) part.get("dialogoiTopothesias"));

            Map<String, Map<String, Integer>> charsPart = (Map<String, Map<String, Integer>>) part.get("lekseisXaraktira");
            for (String c : charsPart.keySet()) {
                sinoloLekseonCharaktiron.putIfAbsent(c, new HashMap<>());
                mergeCounts(sinoloLekseonCharaktiron.get(c), charsPart.get(c));
            }
        }

        executor.shutdown();
        
        long endTime = System.currentTimeMillis();
        System.out.println("Χρονος εκτελεσης: " + (endTime - startTime) + " ms");
        
        // Επεισοδιο με τα περισσοτερα λογια
        String maxEpisode = Collections.max(sinolikesLekseis.entrySet(), Map.Entry.comparingByValue()).getKey();

        // Τοποθεσια με τις περισσθτερες στιχομυθιες
        String maxLocation = Collections.max(dialogoiTopothesias.entrySet(), Map.Entry.comparingByValue()).getKey();

        // Πιο κοινη λεξη για Bart, Homer, Marge, Lisa
        System.out.println("1) Επεισοδιο με τις περισσοτερες λεξεις: " + maxEpisode);
        System.out.println("2) Τοποθεσια με τις περισσοτερες στιχομυθιες: " + maxLocation);

        System.out.println("3) Πιο κοινες λεξεις:");
        for (String c : sinoloLekseonCharaktiron.keySet()) {
            Map.Entry<String, Integer> koines = Collections.max(sinoloLekseonCharaktiron.get(c).entrySet(),
                    Map.Entry.comparingByValue());
            System.out.println(c + ": " + koines.getKey() + " (" + koines.getValue() + " φορες)");
        }
    }

    // Συναρτηση συγχωνευσης
    static void mergeCounts(Map<String, Integer> target, Map<String, Integer> source) {
        for (var e : source.entrySet()) {
            target.merge(e.getKey(), e.getValue(), Integer::sum);
        }
    }

    // Συναρτηση φορτωσης CSV
    static List<grammesKeimenou> loadData(String filename) throws IOException {
        List<grammesKeimenou> lines = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
            String header = br.readLine(); // παραλειψη πρωτης γραμμης
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",", -1);
                if (parts.length < 4) continue;
                lines.add(new grammesKeimenou(parts[1], parts[6], parts[5], parts[7]));
            }
        }
        return lines;
    }
}
