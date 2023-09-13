package com.architectdiagram.controllers;

import com.architectdiagram.dto.ComponentCreateRequest;
import com.architectdiagram.dto.ComponentResponse;
import com.architectdiagram.dto.ComponentType;
import com.architectdiagram.entity.Component;
import com.architectdiagram.entity.impl.KafkaServer;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

/**
 *
 * A component controller handles the lifecycle of a component created by the front end
 */
@RestController
public class ComponentController {
    /**
     *
     * @return the created component
     */
    @RequestMapping(value = "/components", method = RequestMethod.POST)
    @ResponseStatus(HttpStatus.OK)
    public ComponentResponse create(@RequestBody ComponentCreateRequest request) {
        if (request.getType() == ComponentType.kafka) {
            KafkaServer ks = new KafkaServer();
            return new ComponentResponse(ks);
        }
        return null;
    }
}
