package io.kcache.kwack.sqlline;

import sqlline.Application;
import sqlline.BuiltInProperty;
import sqlline.PromptHandler;
import sqlline.SqlLine;
import sqlline.SqlLineOpts;

public class KwackApplication extends Application {

    public KwackApplication() {
        super();
    }

    @Override
    public SqlLineOpts getOpts(SqlLine sqlline) {
        SqlLineOpts opts = super.getOpts(sqlline);
        opts.set(BuiltInProperty.CONNECT_INTERACTION_MODE, "notAskCredentials");
        opts.set(BuiltInProperty.MAX_WIDTH, 120);
        return opts;
    }

    @Override
    public PromptHandler getPromptHandler(SqlLine sqlLine) {
        return new KwackPromptHandler(sqlLine);
    }

    @Override
    public String getInfoMessage() {
        return "Welcome to kwack!\n"
        + "Enter \"!help\" for usage hints.\n\n"
        + "      ___(.)>\n"
        + "~~~~~~\\___)~~~~~~\n";
    }
}
