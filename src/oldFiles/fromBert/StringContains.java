/*
 * This file is part of the PSL software.
 * Copyright 2011 University of Maryland
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.umd.cs.linqs.twitter;

import edu.umd.cs.psl.model.argument.Attribute;
import edu.umd.cs.psl.model.argument.GroundTerm;
import edu.umd.cs.psl.model.argument.type.ArgumentType;
import edu.umd.cs.psl.model.argument.type.ArgumentTypes;
import edu.umd.cs.psl.model.function.ExternalFunction;
import edu.umd.cs.psl.model.predicate.type.PredicateType;
import edu.umd.cs.psl.model.predicate.type.PredicateTypes;
import java.lang.Math;
import java.lang.String;

/**
 * This is an example external function.
 */
class StringContains implements ExternalFunction {
	
	/**
	 * Returns the value for the given arguments in the GroundTerm array. In this
	 * case the arguments are:
     * GroundTerm[0] = score
	 */
	@Override
	public double[] getValue(GroundTerm... args) {
		if (args[0] instanceof Attribute && args[1] instanceof Attribute) {
			String string = (String) ((Attribute) args[0]).getAttribute();
			String pattern = (String) ((Attribute) args[1]).getAttribute();

			//System.out.println(string + ", " + pattern);

			if (string.toLowerCase().contains(pattern.toLowerCase()))
				return new double [] { 1.0 };
			return new double [] { 0.0 };
		}
		else throw new IllegalArgumentException("Strings only.");
	}
	
	/**
	 * Returns the number of arguments to the function.
	 */
	@Override
	public int getArity() {
		return 2;
	}
	
	/**
	 * Returns the type(s) of the argument(s) to the function. There should be as
	 * many types returned as the arity of the function. See {@link ArgumentTypes}
	 * for default types.
	 */
	@Override
	public ArgumentType[] getArgumentTypes() {
		return new ArgumentType[] { ArgumentTypes.Text, ArgumentTypes.Text };
	}
	
	/**
	 * Returns the predicate type associated with this function. See
	 * {@link PredicateTypes} for default types.
	 */
	@Override
	public PredicateType getPredicateType() {
		return PredicateTypes.SoftTruth;
	}
	
}
