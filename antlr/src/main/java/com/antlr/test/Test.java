package com.antlr.test;

import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.tree.ParseTree;

public class Test {

    public static void main(String[] args)throws Exception{
        CharStream charStream = CharStreams.fromString("{12,3,32}");
        ArrayInitLexer lexer = new ArrayInitLexer(charStream);

        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        ArrayInitParser parser = new ArrayInitParser(tokenStream);

        ParseTree tree = parser.init();
        System.out.println(tree.toStringTree(parser));
    }

    static class ShortToUnicodeString extends ArrayInitBaseListener{
        @Override
        public void enterInit(ArrayInitParser.InitContext ctx) {
            System.out.print('"');
        }

        public void exitInit(ArrayInitParser.InitContext ctx){
            System.out.print('"');
        }

        public void enterValue(ArrayInitParser.ValueContext ctx){
            int value = Integer.valueOf(ctx.INT().getText());
            System.out.printf("\\u%o4x",value);

        }

    }


}
