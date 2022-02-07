node program.js show-schema -d tableConfig -t Patient -maxDepth 4
Table Name: Patient
- ColumnName: ResourceId                               Type: string
- ColumnName: Active                                   Type: boolean
- ColumnName: NameText                                 Type: string
- ColumnName: NameFamily                               Type: string
- ColumnName: Namegiven                                Type: string
- ColumnName: Nameprefix                               Type: string
- ColumnName: Namesuffix                               Type: string
- ColumnName: Gender                                   Type: code
- ColumnName: BirthDate                                Type: date
- ColumnName: MaritalStatusCodingSystem                Type: uri
- ColumnName: MaritalStatusCodingCode                  Type: code
- ColumnName: MaritalStatusCodingDisplay               Type: string
- ColumnName: MaritalStatusText                        Type: string
- ColumnName: CommunicationLanguageText                Type: string
- ColumnName: CommunicationLanguageCodingSystem        Type: string
- ColumnName: CommunicationLanguageCodingCode          Type: string
- ColumnName: DeceasedBoolean                          Type: boolean
- ColumnName: DeceasedDateTime                         Type: dateTime
- ColumnName: MultipleBirthBoolean                     Type: boolean

Table Name: Practitioner
- ColumnName: ResourceId                               Type: string
- ColumnName: Active                                   Type: boolean
- ColumnName: NameText                                 Type: string
- ColumnName: NameFamily                               Type: string
- ColumnName: Namegiven                                Type: string
- ColumnName: Nameprefix                               Type: string
- ColumnName: Namesuffix                               Type: string
- ColumnName: TelecomSystem                            Type: code
- ColumnName: TelecomValue                             Type: string
- ColumnName: TelecomUse                               Type: code