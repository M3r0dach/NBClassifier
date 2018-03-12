package nbc;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class NBClassifier {
	public static void train(String trainingData, String modelFile) throws Exception {
		if ((new File(modelFile)).exists()) {
			return;
		} else {
			/***********************************************************/
			/*
			 * 在else中添加由MapReduce实现的模型训练的代码。
			 * 注意：
			 *     在模型训练完成之后，需要将在Hadoop上训练得到的模型文件数据（一般其名称
			 *     为"part-r-00000"）下载到指定的本地目录下，该目录与modelFile相同，
			 *     名称与modelFile相同
			 */
			
			/***********************************************************/
			String[] args = {trainingData, modelFile};
			NBTrainingDriver.main(args);
			System.out.println("Jump to Classifer");
		}
	}
	
	public static HashMap<String, Integer> prior = null;
	public static double priorNorm = 0.;
	public static HashMap<String, Integer> likelihood = null;
	public static HashMap<String, Double> likelihoodNorm = null;
	public static HashSet<String> V = null;
	
	public static void loadModel(String modelFile) throws Exception {
		if (prior != null && likelihood != null) {
			return;
		}
		
		prior = new HashMap<String, Integer>();
		likelihood = new HashMap<String, Integer>();
		likelihoodNorm = new HashMap<String, Double>();
		V = new HashSet<String>();
		
		BufferedReader br = new BufferedReader(new FileReader(modelFile));
		String line = null;
		
		while ((line = br.readLine()) != null) {
			String feature = line.substring(0, line.indexOf("\t"));
			Integer count = Integer.parseInt(line.substring(line.indexOf("\t") + 1));
			
			if (feature.contains("-")) {
				likelihood.put(feature, count);
				
				String label = feature.substring(0, feature.indexOf("-"));
				
				if (likelihoodNorm.containsKey(label)) {
					likelihoodNorm.put(label, likelihoodNorm.get(label) + (count + 0.));
				} else {
					likelihoodNorm.put(label, count + 0.);
				}
				
				String word = feature.substring(feature.indexOf("-") + 1);
				
				if (!V.contains(word)) {
					V.add(word);
				}
			} else {
				prior.put(feature, count);
				priorNorm += count;
			}
		}
		
		br.close();
	}
	
	public static String predict(String sentence, String modelFile) throws Exception {
		loadModel(modelFile);
		
		String predLabel = null;
		double maxValue = Double.NEGATIVE_INFINITY;
		
		String[] words = sentence.split(" ");
		Set<String> labelSet = prior.keySet();
		
		for (String label : labelSet) {
			double tempValue = Math.log(prior.get(label) / priorNorm);
			
			for (String word : words) {
				if (!V.contains(word)) {
					continue;
				}
				
				String pseudoW = label + "-" + word;
				
				if (likelihood.containsKey(pseudoW)) {
					tempValue += Math.log((likelihood.get(pseudoW) + 1) / (likelihoodNorm.get(label) + V.size()));
				} else {
					tempValue += Math.log(1 / (likelihoodNorm.get(label) + V.size()));
				}
			}
			
			if (tempValue > maxValue) {
				maxValue = tempValue;
				predLabel = label;
			}
		}
		
		return predLabel;
	}
	
	public static void validate(String sentencesFile, String modelFile, String resultFile) throws Exception {
		BufferedReader br = new BufferedReader(new FileReader(sentencesFile));
		String sentence = null;
		
		BufferedWriter bw = new BufferedWriter(new FileWriter(resultFile));
		
		while ((sentence = br.readLine()) != null) {
			String predLabel = predict(sentence, modelFile);
			
			bw.append(predLabel + "\r\n");
		}
		
		br.close();
		bw.flush();
		bw.close();
	}
	
	public static void main(String[] args) throws Exception {
		String trainingFile = "/input_2015081018"; // 这里指定hdfs下training data的scheme
		String modelFile = "d:/NBClassifier/model.txt"; // 这里指定model文件将放置在本地的那个目录下，并给model文件命名
		NBClassifier.train(trainingFile, modelFile);
		
		if ((new File(modelFile)).exists()) {
			String sentencesFile = "d:/NBClassifier/sentences.txt"; // 这里指定sentences.txt文件的路径
			String resultFile = "d:/NBClassifier/2015081018_预测结果.txt"; // 这里指定结果文件路径，结果文件的名称按照指定要求描述
			NBClassifier.validate(sentencesFile, modelFile, resultFile);
		}
		else {
			System.out.println("model文件不存在");
		}
	}
}
