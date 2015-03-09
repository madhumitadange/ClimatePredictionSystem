import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

public class ChowLiuLearning {

	public static void main(String args[]) throws Exception {
		double startTime = System.currentTimeMillis();
		if(args.length < 3) {
			throw new Exception("Please pass input network, train, test and output network files as program arguments.");
		}
		NetworkManager mgr = new NetworkManagerImpl();
		int[] domainSizes = getDomainSizes(args[2]);
		Map<int[], Double> dataSet = mgr.gatherSamplesFromDataSet(args[0]);
//		Network origNetwork = mgr.createNetwork(args[0]);
		Network lrndNetwork = mgr.learnNetworkStructureChowLiu(dataSet, domainSizes);
		mgr.learnNetworkParametersFODMLE(lrndNetwork, dataSet);
		lrndNetwork.writeToFile(args[1]);
//		double lld = mgr.getLogLikelihoodDifference(origNetwork, lrndNetwork, args[2]);

//		Util.log("Log Likelihood Difference: "+lld);
 		Util.log("ChowLiuLearning: Total execution time->"+((double)(System.currentTimeMillis()-startTime)/1000)+" secs.");
	}
	
	private static int[] getDomainSizes(String filePath) throws IOException {
		int[] domainSizes;
		BufferedReader br = new BufferedReader(new FileReader(filePath));
		String str = br.readLine();
		int varCnt = Integer.parseInt(str.trim());
		domainSizes = new int[varCnt];
		int i = 0;
		while((str = br.readLine()) != null) {
			domainSizes[i] = Integer.parseInt(str.substring(str.indexOf(",")+1).trim());
			i++;
		}
		br.close();
		return domainSizes;
	}

}
