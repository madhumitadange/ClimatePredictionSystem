package com.cps.ais;

import java.io.BufferedWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Function {

	List<Variable> variables;
	Object values;

	public Function() {
		this.variables = new ArrayList<Variable>();
	}

	public void initializeParamStructure() {
		this.values = new Object[this.variables.get(0).domainSize];
		initializeParams(this.values, 0);
	}

	private void initializeParams(Object obj, int depth) {
		if (depth == this.variables.size() - 1) {
			for (int i = 0; i < this.variables.get(depth).domainSize; i++) {
				((Object[]) obj)[i] = Double.parseDouble("1");
			}
		} else {
			for (int i = 0; i < this.variables.get(depth).domainSize; i++) {
				((Object[]) obj)[i] = new Object[this.variables.get(depth + 1).domainSize];
				initializeParams(((Object[]) obj)[i], depth + 1);
			}
		}
	}

	public Function clone(boolean isData, int initParam) {
		Function f = new Function();
		f.variables = new ArrayList<Variable>();
		for (int i = 0; i < this.variables.size(); i++) {
			f.variables.add(this.variables.get(i));
		}
		// clone values with dummy values at leaves.
		f.values = new Object[((Object[]) this.values).length];
		cloneRec(this.values, f.values, 0, isData, initParam);
		return f;
	}

	public void cloneRec(Object src, Object trg, int depth, boolean isData,
			int initParam) {
		int varDomainSize = ((Object[]) src).length;
		if (depth == this.variables.size() - 1) {
			for (int i = 0; i < varDomainSize; i++) {
				if (isData) {
					((Object[]) trg)[i] = new Double(
							(Double) ((Object[]) src)[i]);
				} else {
					((Object[]) trg)[i] = new Double(initParam);
				}
			}
		} else {
			for (int i = 0; i < varDomainSize; i++) {
				((Object[]) trg)[i] = new Object[((Object[]) ((Object[]) src)[i]).length];
				cloneRec(((Object[]) src)[i], ((Object[]) trg)[i], depth + 1,
						isData, initParam);
			}
		}
	}

	public Function multiply(Function f) throws Exception {
		int[] conf = new int[this.variables.size()];
		this.values = visitValues(0, conf, this.values, f);
		for (int i = 0; i < f.variables.size(); i++) {
			if (this.variables.indexOf(f.variables.get(i)) == -1) {
				this.variables.add(f.variables.get(i));
			}
		}
		return this;
	}

	private Object visitValues(int level, int[] conf, Object value, Function f)
			throws Exception {
		if (value instanceof Double) {
			double d = (Double) value;
			value = generateValues(d, conf, f, f.values, 0);
		} else {
			for (int i = 0; i < ((Object[]) value).length; i++) {
				conf[level] = i;
				((Object[]) value)[i] = visitValues(level + 1, conf,
						((Object[]) value)[i], f);
			}
		}
		return value;
	}

	private Object generateValues(double d, int[] conf, Function f,
			Object value, int level) throws Exception {
		Object newValue = null;
		if (value instanceof Double) {
			newValue = d * ((Double) value);
		} else if (this.variables.indexOf(f.variables.get(level)) == -1) {
			newValue = new Object[((Object[]) value).length];
			for (int i = 0; i < ((Object[]) value).length; i++) {
				((Object[]) newValue)[i] = generateValues(d, conf, f,
						((Object[]) value)[i], level + 1);
			}
		} else {
			newValue = generateValues(d, conf, f,
					((Object[]) value)[conf[this.variables.indexOf(f.variables
							.get(level))]], level + 1);
		}
		return newValue;
	}

	public void sumOutVariable(int vIndex) throws Exception {
		int vPos = this.variables.indexOf(vIndex);
		this.values = sumOut(0, vPos, this.values);
		this.variables.remove(new Integer(vIndex));
	}

	private Object sumOut(int level, int vPos, Object value) throws Exception {
		if (level == vPos) {
			for (int i = 1; i < ((Object[]) value).length; i++) {
				((Object[]) value)[0] = add(((Object[]) value)[0],
						((Object[]) value)[i]);
			}
			value = ((Object[]) value)[0];
		} else {
			for (int i = 0; i < ((Object[]) value).length; i++) {
				((Object[]) value)[i] = sumOut(level + 1, vPos,
						((Object[]) value)[i]);
			}
		}
		return value;
	}

	public Object add(Object obj1, Object obj2) throws Exception {
		if (obj1 instanceof Double) {
			return ((Double) obj1 + (Double) obj2);
		} else {
			for (int i = 0; i < ((Object[]) obj1).length; i++) {
				((Object[]) obj1)[i] = add(((Object[]) obj1)[i],
						((Object[]) obj2)[i]);
			}
			return obj1;
		}
	}

	public static List<Integer> getFnVariablesFromFnDefn(String[] fnDefn) {
		List<Integer> l = new ArrayList<Integer>();
		for (int i = 1; i < fnDefn.length; i++) {
			l.add(Integer.parseInt(fnDefn[i].trim()));
		}
		return l;
	}

	public void printVals(Object obj, int height, int curDepth,
			BufferedWriter bw) throws Exception {
		int varDomainSize = ((Object[]) obj).length;
		if (curDepth == (height - 1)) {
			for (int i = 0; i < varDomainSize; i++) {
				bw.write((Double) ((Object[]) obj)[i] + " ");
			}
			bw.newLine();
		} else {
			for (int i = 0; i < varDomainSize; i++) {
				printVals(((Object[]) obj)[i], height, curDepth + 1, bw);
			}
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void printVals(Object obj, int height, int curDepth, Context context)
			throws Exception {
		int varDomainSize = ((Object[]) obj).length;
		if (curDepth == (height - 1)) {
			String op = "";
			for (int i = 0; i < varDomainSize; i++) {
				op = op + ((Double) ((Object[]) obj)[i]) + " ";
			}
			context.write(new Text(""), new Text(op));
		} else {
			for (int i = 0; i < varDomainSize; i++) {
				printVals(((Object[]) obj)[i], height, curDepth + 1, context);
			}
		}
	}

	public double getLogLikelihood(int[] sample) {
		return Math.log10(getLikelihood(sample));// Math.log10(2);
	}

	public double getLikelihood(int[] sample) {
		return getSampleValue(this.values, sample, 0);
	}

	private double getSampleValue(Object obj, int[] sample, int depth) {
		if (depth == (this.variables.size() - 1)) {
			return (Double) ((Object[]) obj)[sample[this.variables.get(depth).index]];
		} else {
			return getSampleValue(
					((Object[]) obj)[sample[this.variables.get(depth).index]],
					sample, depth + 1);
		}
	}

	public void updateSampleValue(Object obj, int[] sample, double value,
			int depth) {
		if (depth == (this.variables.size() - 1)) {
			((Object[]) obj)[sample[this.variables.get(depth).index]] = (Double) ((Object[]) obj)[sample[this.variables
					.get(depth).index]] + value;
		} else {
			updateSampleValue(
					((Object[]) obj)[sample[this.variables.get(depth).index]],
					sample, value, depth + 1);
		}
	}

	public void normalizeParameters(Object obj, int depth) {
		int varDomainSize = ((Object[]) obj).length;
		if (depth == (this.variables.size() - 1)) {
			double total = 0;
			for (int i = 0; i < varDomainSize; i++) {
				total = total + (Double) ((Object[]) obj)[i];
			}
			for (int i = 0; i < varDomainSize; i++) {
				((Object[]) obj)[i] = (total == 0 ? 0
						: (((Double) ((Object[]) obj)[i]) / total));
			}
		} else {
			for (int i = 0; i < varDomainSize; i++) {
				normalizeParameters(((Object[]) obj)[i], depth + 1);
			}
		}
	}

	public void resetParameters(Object obj, int depth) {
		int varDomainSize = ((Object[]) obj).length;
		if (depth == (this.variables.size() - 1)) {
			for (int i = 0; i < varDomainSize; i++) {
				((Object[]) obj)[i] = Double.parseDouble("1");
			}
		} else {
			for (int i = 0; i < varDomainSize; i++) {
				resetParameters(((Object[]) obj)[i], depth + 1);
			}
		}
	}

	public void updateParametersAdaptiveImportance(Object src, Object temp,
			double alpha, int depth) {
		int varDomainSize = ((Object[]) src).length;
		if (depth == (this.variables.size() - 1)) {
			double total = 0;
			for (int i = 0; i < varDomainSize; i++) {
				total = total + (Double) ((Object[]) temp)[i];
			}
			for (int i = 0; i < varDomainSize; i++) {
				if (total != 0) {
					((Object[]) src)[i] = ((Double) ((Object[]) src)[i])
							* (1 - alpha) + ((Double) ((Object[]) temp)[i])
							* alpha;
				}
			}
		} else {
			for (int i = 0; i < varDomainSize; i++) {
				updateParametersAdaptiveImportance(((Object[]) src)[i],
						((Object[]) temp)[i], alpha, depth + 1);
			}
		}
	}

}
