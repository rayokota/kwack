package io.kcache.kwack.sqlline;

import sqlline.Application;
import sqlline.BuiltInProperty;
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
        return opts;
    }

    @Override
    public String getInfoMessage() {
        return "Welcome to kwack!\n"
        + "      ___(.)>\n"
        + "~~~~~~\\___)~~~~~~\n";
    }
}
