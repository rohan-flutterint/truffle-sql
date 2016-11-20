package com.fivetran.truffle;

import com.oracle.truffle.api.dsl.NodeChild;
import com.oracle.truffle.api.dsl.NodeChildren;

@NodeChildren({@NodeChild("leftNode"), @NodeChild("rightNode")})
abstract class ExprBinary extends ExprBase {
}
