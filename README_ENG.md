# Parallel Analysis of Simpsons Dialogues (Java)

This project extracts statistical information from the *Simpsons* TV series using multithreaded processing in Java.

The input dataset (`simpsons_script_lines.csv`) contains dialogue lines from all episodes, including information such as episode, speaking character, location, and dialogue text.

The program loads the dataset into memory and processes it in parallel using k threads.

## 📚 Information
- Course: PLH 47
- Assignment: Parallel Simpsons Dataset Analysis
- Language: Java
  
## 📝 Description

The program reads all lines from the file `simpsons_script_lines.csv` into an array.  
Then, k threads (where k is a power of 2) are created, each responsible for processing a portion of the dataset.

The following statistics are computed:

1. The episode with the highest total number of spoken words  
2. The location with the largest number of dialogue exchanges  
3. For each of the characters **Bart**, **Homer**, **Marge**, and **Lisa**:
   - The most frequently used word (with length ≥ 5 characters)
   - The number of times that word appears

HashMaps are used to aggregate intermediate results, which are later merged into global structures.

The program also measures execution time for different thread counts.

## 🛠️ Technologies
- Java
- Java Threads
- CSV file processing
- HashMap / ConcurrentHashMap
- System.nanoTime() for performance measurements
