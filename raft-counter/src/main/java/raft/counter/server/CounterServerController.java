package raft.counter.server;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value="/v1/counter")
public class CounterServerController {

    @RequestMapping(value = "/increaseAndGet", method = RequestMethod.POST)
    public int increaseAndGet(){
        return 1100;
    }
}
