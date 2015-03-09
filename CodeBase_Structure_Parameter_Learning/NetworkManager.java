import java.util.List;
import java.util.Map;

public interface NetworkManager {

	public Network createNetwork(String networkFilePath) throws Exception;
	
	public void clampEvidence(Network network, String evidenceFilePath) throws Exception;

	public double computeLikelihoodWeightingBasedPOE(Network P, Network Q, double alpha, int I, int N) throws Exception;

	public double computeLikelihoodWeightingBasedPOEParallel(Network P, Network Q, double alpha, int I, int N) throws Exception;

	public double computeLikelihoodWeightingBasedPOEDynamicAlpha(Network P, Network Q, double minAlpha, double maxAlpha, int I, int N) throws Exception;
	
	public Map<Integer, Integer> generateSample(Network network) throws Exception;
	
	public double getSampleLikelihood(Network network, Map<Integer, Integer> sample);

	public void instantiateEvidence(Network network, String evidenceFilePath) throws Exception;

	public void instantiateEvidence(Network network, Map<Integer, Integer> evidence) throws Exception;

	public double computePOE(Network network) throws Exception;

	public void computeMinDegreeOrdering(Network network) throws Exception;

	public List<Integer> generateWCutSet(Network network, int w) throws Exception;

	public double computeSamplingBasedPOE(String args[]) throws Exception;

	public double computeSamplingBasedPOE(String uaiFile, String evidFile, int w, int N, boolean isAdaptive) throws Exception;
	
	public double computePartialSamplingBasedPOE(String uaiFile, String evidFile, int w, double alpha, int I, int N) throws Exception;
	
	public double computeSamplingBasedPOEAdaptive(Network P, String evidFile, double alpha, int I, int N) throws Exception;

	public Map<int[], Double> gatherSamplesFromDataSet(String fileName) throws Exception;
	
	public Network learnNetworkStructureChowLiu(Map<int[], Double> data, int[] domainSizes) throws Exception;
	
	public void learnNetworkParametersFODMLE(Network network, Map<int[], Double> data) throws Exception;
	
	public void learnNetworkParametersPODEM(Network network, Map<int[], Double> data) throws Exception;
	
	public double getLogLikelihoodDifference(Network origNw, Network lrndNw, String testFileName) throws Exception;
}
