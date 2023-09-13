package com.architectdiagram.dto;

import com.architectdiagram.entity.Component;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class ComponentResponse {
    private Component component;
}
