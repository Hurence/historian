package com.hurence.historian.job;


import com.github.rvesse.airline.SingleCommand;
import com.github.rvesse.airline.annotations.Arguments;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;

import java.util.List;

@Command(name = "historian-launcher", description = "Manage state of the historian server")
public class StatusHistorianServerCommand {

    @Option(name = { "-f", "--flag" }, description = "An option that requires no values")
    private boolean flag = false;

    @Arguments(description = "Additional arguments")
    private List<String> args;

    public static void main(String[] args) {
        SingleCommand<StatusHistorianServerCommand> parser = SingleCommand.singleCommand(StatusHistorianServerCommand.class);
        StatusHistorianServerCommand cmd = parser.parse(args);
        cmd.run();
    }

    private void run() {
        System.out.println("Flag was " + (this.flag ? "set" : "not set"));
        if (args != null)
            System.out.println("Arguments were " + StringUtils.join(args, ","));
    }
}

