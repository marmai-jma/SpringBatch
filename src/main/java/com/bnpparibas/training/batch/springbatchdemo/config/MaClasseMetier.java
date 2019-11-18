package com.bnpparibas.training.batch.springbatchdemo.config;

import com.bnpparibas.training.batch.springbatchdemo.dto.BookDto;
import org.springframework.stereotype.Service;

@Service
public class MaClasseMetier {
    public BookDto maMethodeMetier(final BookDto bookDto){
        if (bookDto.getPublishedOn()> 2019){
            return null;
        }
        return bookDto;
    }
}
