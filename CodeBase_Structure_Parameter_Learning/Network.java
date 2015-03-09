import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

enum NetworkType {
	MARKOV,
	BAYES
}

public class Network {
	Variable[] variables;
	Map<Integer, Function> functions;	//Using Map instead of List because, when deletion of functions happens the whole list of functions indexes gets modified.
	NetworkType type;
	int fnSequence;
	Map<Integer, Integer> evidence;
	List<Integer> minDegreeOrder;
	int treeWidth;
	
	public Network() {
		this.evidence = new LinkedHashMap<Integer, Integer>();
		this.minDegreeOrder = new ArrayList<Integer>();
		this.treeWidth = 0;
	}
	
	//Used only for ChowLiu algorithm while identifying the structure of the network.
	public void correctFunctionsVariableOrdering(int var, List<Variable> path) {
		Iterator<Integer> itr = this.variables[var].functionRefs.iterator();
		while(itr.hasNext()) {
			int fnIndex = itr.next();
			Function f = this.functions.get(fnIndex);
			if((f.variables.size() == 2) && (f.variables.get(1).index == var) && (!path.contains(f.variables.get(0)))) {
				List<Variable> newVariableOrder = new ArrayList<Variable>();
				newVariableOrder.add(f.variables.get(1));
				newVariableOrder.add(f.variables.get(0));
				f.variables = newVariableOrder;
				List<Variable> newPath = new ArrayList<Variable>(path);
				newPath.add(f.variables.get(0));
				correctFunctionsVariableOrdering(f.variables.get(1).index, newPath);
			}
		}
	}
	
	public boolean isPathExists(int src, int trg, String visited) {
		Iterator<Variable> itr = this.variables[src].connections.iterator();
		while(itr.hasNext()) {
			int conn = itr.next().index;
			if((src == conn) || (visited.indexOf(":"+conn+":") != -1)) {
				continue;
			}
			if(conn == trg) {
				return true;
			} else {
				String newVisited = new String(visited);
				newVisited = newVisited + ":"+src+":";
				return isPathExists(conn, trg, newVisited);
			}
		}
		return false;
	}
	
	public double getLogLikelihood(int[] sample) {
		double lld = 0.0;
		Iterator<Function> itr = this.functions.values().iterator();
		while(itr.hasNext()) {
			Function fn = itr.next();
			lld = lld + fn.getLogLikelihood(sample);
		}
		return lld;
	}

	public double getLikelihood(int[] sample) {
		double lld = 1.0;
		Iterator<Function> itr = this.functions.values().iterator();
		while(itr.hasNext()) {
			Function fn = itr.next();
			lld = lld * fn.getLikelihood(sample);
		}
		return lld;
	}

	public void writeToFile(String fileName) throws Exception {
		BufferedWriter bw = new BufferedWriter(new FileWriter(fileName));

		if(this.type == NetworkType.BAYES) {
			bw.write("BAYES");
		} else {
			bw.write("MARKOV");
		}
		bw.newLine();
		
		bw.write(this.variables.length+"");
		bw.newLine();
		
		for(int i = 0; i < this.variables.length; i++) {
			bw.write(this.variables[i].domainSize+" ");
		}
		bw.newLine();
		
		bw.write(this.functions.size()+"");
		bw.newLine();
		Iterator<Function> itr = this.functions.values().iterator();
		while(itr.hasNext()) {
			Function fn = itr.next();
			bw.write(fn.variables.size() + "\t");
			for(Variable i: fn.variables) {
				bw.write(i.index + "\t");
			}
			bw.newLine();
		}
		
		itr = this.functions.values().iterator();
		while(itr.hasNext()) {
			Function fn = itr.next();
			int paramCnt = getParamCnt(fn.variables);
			bw.newLine();
			bw.write(paramCnt + "");
			bw.newLine();
			fn.printVals(fn.values, fn.variables.size(), 0, bw);
		}

		bw.flush();
		bw.close();
	}
	
	private int getParamCnt(List<Variable> variables) {
		int paramCnt = 1;
		for(Variable i: variables) {
			paramCnt = paramCnt * this.variables[i.index].domainSize;
		}
		return paramCnt;
	}

	public Network clone(boolean isData, int initParam) {
		Network network = new Network();

		network.variables = new Variable[this.variables.length];
		for(int i = 0; i < this.variables.length; i++) {
			network.variables[i] = this.variables[i].clone();
		}
		
		network.functions = new LinkedHashMap<Integer, Function>();
		Iterator<Integer> itr = this.functions.keySet().iterator();
		while(itr.hasNext()) {
			Integer key = itr.next();
			network.functions.put(key, this.functions.get(key).clone(isData, initParam));
		}
		
		network.type = this.type;
		network.fnSequence = this.fnSequence;
		
		itr = this.evidence.keySet().iterator();
		while(itr.hasNext()) {
			Integer key = itr.next();
			network.evidence.put(key, this.evidence.get(key));
		}
		
		return network;
	}
	
	public int getNextMinDegreeVariableIndex() throws Exception {
		int minDegree = Integer.MAX_VALUE;
		int minDegreeVarRef = -1;
		for(int i = 0; i < this.variables.length; i++) {
			if(this.variables[i].functionRefs.size() == 0)
				continue;
			int degree = this.variables[i].connections.size();
			if(this.variables[i].connections.contains(i)) {
				degree--;
			}
			if(degree < minDegree) {
				minDegree = degree;
				minDegreeVarRef = i;
			}
		}
		return minDegreeVarRef;
	}

	public int getMinScopeFunction(List<Integer> funcRefs) throws Exception {
		int minScope = Integer.MAX_VALUE;
		int minScopeFnIndex = -1;
		for(int i: funcRefs) {
			if(this.functions.get(i).variables.size() < minScope) {
				minScope = this.functions.get(i).variables.size();
				minScopeFnIndex = i;
			}
		}
		return minScopeFnIndex;
	}

	public void populateFunction(Function f, int fnVarIndex, Object obj, String[] fnVals, int sIndex, int size) throws Exception {
		int varDomainSize = this.variables[f.variables.get(fnVarIndex).index].domainSize;
		if(fnVarIndex == f.variables.size()-1) {
			for(int i = 0; i < varDomainSize; i++) {
				((Object[]) obj)[i] = Double.parseDouble(fnVals[sIndex+i]);
			}
		} else {
			for(int i = 0; i < varDomainSize; i++) {
				((Object[]) obj)[i] = new Object[this.variables[f.variables.get(fnVarIndex+1).index].domainSize];
				populateFunction(f, fnVarIndex+1, ((Object[]) obj)[i], fnVals, sIndex + (size*i/varDomainSize), size/varDomainSize);
			}
		}
	}

	public void productAndSumOut(int vIndex) throws Exception {
		int opFnIndex = this.productFunctions(this.variables[vIndex].functionRefs);
		this.sumOutVariableFromFunction(opFnIndex, vIndex);	//Should be in Function.java
	}

	/* WORKING:
	 * Multiply 2 functions at a time, till only one is left at the end.
	 * Before calling multiply on 2 functions
	 * - remove those function references from all the variables.
	 * - decrement the degree value in all the variables.
	 * - remove the argument function from network function list.
	 * - save the index of the this function.
	 * Generate the product. f1.multiply(f2);
	 * After the product is returned
	 * - add the above saved this function index in all the references of all the variables in the modified function.
	 * - increment the degree value in all the variables in the modified function.
	 */
	public int productFunctions(List<Integer> functionRefs) throws Exception {
		List<Integer> funcRefs = new ArrayList<Integer>(functionRefs);
		while(funcRefs.size() > 1) {
			int f1Index = this.getMinScopeFunction(funcRefs);
			Integer F1Index = new Integer(f1Index);
			funcRefs.remove(F1Index);
			int f2Index = this.getMinScopeFunction(funcRefs);
			Integer F2Index = new Integer(f2Index);
			funcRefs.remove(F2Index);
			funcRefs.add(F1Index);
			
			for(Variable i: this.functions.get(f1Index).variables) {
				this.variables[i.index].functionRefs.remove(F1Index);
				this.variables[i.index].connections.removeAll(this.functions.get(f1Index).variables);
			}
			for(Variable i: this.functions.get(f2Index).variables) {
				this.variables[i.index].functionRefs.remove(F2Index);
				this.variables[i.index].connections.removeAll(this.functions.get(f2Index).variables);
			}
			Function f1 = this.functions.get(f1Index);
			Function f2 = this.functions.get(f2Index);
			this.functions.remove(f2Index);
			f1.multiply(f2);
			for(Variable i: this.functions.get(f1Index).variables) {
				this.variables[i.index].functionRefs.add(F1Index);
				this.variables[i.index].connections.addAll(this.functions.get(f1Index).variables);
			}
		}
		return funcRefs.get(0);
	}

	public void sumOutVariableFromFunction(int fnIndex, int vIndex) throws Exception {
		Integer FnIndex = new Integer(fnIndex);
		Integer VIndex = new Integer(vIndex);
		
		this.functions.get(fnIndex).sumOutVariable(vIndex);
		if(this.variables[vIndex].functionRefs.size() != 1) {
			throw new Exception("Variable "+vIndex+" function list size must have been 1 by now. Please check for any potential issues.");
		}
		this.variables[vIndex].connections.clear();
		this.variables[vIndex].functionRefs.remove(FnIndex);
		for(Variable i: this.functions.get(fnIndex).variables) {
			this.variables[i.index].connections.remove(this.variables[VIndex]);
		}
	}

}
