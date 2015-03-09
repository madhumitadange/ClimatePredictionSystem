import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Variable {
	int index;
	int domainSize;
	Set<Variable> connections;
	List<Integer> functionRefs;
	boolean isEvidence;
	int value;
	boolean isSummedOut;
	
	public Variable(int index) {
		this.index = index;
		this.connections = new HashSet<Variable>();
		this.functionRefs = new ArrayList<Integer>();
		this.isEvidence = false;
		this.value = -1;
		this.isSummedOut = false;
	}
	
	public Variable clone() {
		Variable var = new Variable(-1);

		var.index = this.index;
		var.domainSize = this.domainSize;
		var.connections.addAll(this.connections);
		var.functionRefs.addAll(this.functionRefs);
		var.isEvidence = this.isEvidence;
		var.value = this.value;
		var.isSummedOut = this.isSummedOut;
		
		return var;
	}
}
