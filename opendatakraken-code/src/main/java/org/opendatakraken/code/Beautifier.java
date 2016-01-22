package org.opendatakraken.code;

import java.util.StringTokenizer;

import org.slf4j.LoggerFactory; 
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.tree.*;
import org.apache.commons.lang3.StringUtils;
import org.opendatakraken.antlr.*;

public class Beautifier {

	static final org.slf4j.Logger logger = LoggerFactory.getLogger(Beautifier.class);
	
	private String originalText;
	private String beautifiedText = "";
	
	private int indentSpaces = 2;
	
	public void setOriginalText(String text) {
		originalText = text;
	}
	
	public String getBeautifiedText(String text) {
		return beautifiedText;
	}

	public void process() {
		
        logger.info("########################################");
    	logger.info("BEAUTIFYING CODE...");
    	
    	ANTLRInputStream inputStream = new ANTLRInputStream(this.originalText);
    	plsqlLexer lexer = new plsqlLexer(inputStream);
        TokenStream tokens = new CommonTokenStream(lexer);
        plsqlParser parser = new plsqlParser(tokens);
        ParserRuleContext tree = parser.compilation_unit();
        
        logger.debug(String.valueOf(tree.getChildCount()));
        logger.debug(tree.toStringTree(parser));
        navTree(tree, -1);
        
        /*plsqlListener listener = new plsqlBaseListener();
        ParseTreeWalker.DEFAULT.walk(listener, tree);*/
    	
    	/*for (
    		Token token = lexer.nextToken();
    		token.getType() != Token.EOF;
    		token = lexer.nextToken())
    	{
    		logger.debug("Type: " + token.getType());
    		logger.debug("Token: " + token.getText());
    	}*/
		
		/*String openingParentheses = "([{";
		String closingParentheses = ")]}";
		
		int indent = 0;
		
		StringTokenizer st = new StringTokenizer(this.originalText, " ()[]{}", true);
		while(st.hasMoreTokens()) {
			
			String token = st.nextToken();
			//logger.debug(token);
			
			if (openingParentheses.contains(token)) {
				logger.debug("Open parentheses");
				this.beautifiedText += token;
				indent += 1;
			}
			else if (closingParentheses.contains(token)) {
				logger.debug("Close parentheses");
				indent -= 1;
				this.beautifiedText += "\n" + StringUtils.repeat(StringUtils.repeat("  ",indentSpaces), indent) + token;
			}
			else {
				this.beautifiedText += "\n" + StringUtils.repeat(StringUtils.repeat("  ",indentSpaces), indent) + token;
			}
		}*/
		logger.debug(this.beautifiedText);
		
    	logger.info("CODE BEAUTIFIED");
        logger.info("########################################");
	}
	
	private void navTree(ParseTree subTree, int indent) {
		
		int newIndent = indent;
		
		int nChildren = subTree.getChildCount();
		if (nChildren > 0) {
			for (int i = 0; i < nChildren; i++) {
				if (
					subTree.getChild(i).getChildCount() == 0 &&
					subTree.getText() != null &&
					!subTree.getText().equals("") &&
					!subTree.getText().equals(";") &&
					!subTree.getText().equals(",") &&
					!subTree.getText().equalsIgnoreCase("<EOF>")
				) {
					newIndent = indent + 1;
				}
			}
			for (int i = 0; i < nChildren; i++) {
				navTree(subTree.getChild(i), newIndent);
			}
		}
		else {
			if (
				subTree.getText() != null &&
				!subTree.getText().equals("") &&
				!subTree.getText().equals(";") &&
				!subTree.getText().equals(",") &&
				!subTree.getText().equalsIgnoreCase("<EOF>")
			) {
				beautifiedText += "\n" + StringUtils.repeat(StringUtils.repeat(" ",indentSpaces), newIndent);
			}
			if (!subTree.getText().equalsIgnoreCase("<EOF>")) {
				beautifiedText += subTree.getText();
			}
		}
	}
}
